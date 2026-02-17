const express = require('express');
const router = express.Router();

const { db } = require('../db/database');
const { generateReference, generateMomoReferenceId } = require('../services/referenceGen');
const { maskPhone, getTodayDateString, isValidDateString, offeringsToCSV } = require('../utils/helpers');
const momoService = require('../services/momoService');

// --- Prepared statements ---

const getCategoryByCode = db.prepare('SELECT * FROM categories WHERE code = ?');
const getAllCategories = db.prepare('SELECT code FROM categories');

const insertOffering = db.prepare(`
  INSERT INTO offerings (reference_number, momo_reference_id, category_code, amount, phone_number, member_name, member_id, payer_message, status)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
`);

const upsertDailySummary = db.prepare(`
  INSERT INTO daily_summaries (summary_date, category_code, total_count, total_amount)
  VALUES (?, ?, 1, ?)
  ON CONFLICT(summary_date, category_code)
  DO UPDATE SET total_count = total_count + 1, total_amount = total_amount + excluded.total_amount
`);

const getOfferingsByDate = db.prepare(`
  SELECT o.*, c.name as category_name
  FROM offerings o
  JOIN categories c ON o.category_code = c.code
  WHERE DATE(o.created_at) = ?
  ORDER BY o.created_at DESC
`);

const getOfferingsByDateAndCategory = db.prepare(`
  SELECT o.*, c.name as category_name
  FROM offerings o
  JOIN categories c ON o.category_code = c.code
  WHERE o.category_code = ? AND DATE(o.created_at) = ?
  ORDER BY o.created_at DESC
`);

const getSummaryByDate = db.prepare(`
  SELECT
    o.category_code as code,
    c.name,
    COUNT(*) as count,
    SUM(o.amount) as total,
    SUM(CASE WHEN o.status = 'SUCCESSFUL' THEN 1 ELSE 0 END) as successCount,
    SUM(CASE WHEN o.status = 'FAILED' THEN 1 ELSE 0 END) as failedCount
  FROM offerings o
  JOIN categories c ON o.category_code = c.code
  WHERE DATE(o.created_at) = ?
  GROUP BY o.category_code
  ORDER BY total DESC
`);

const getOfferingByRef = db.prepare('SELECT * FROM offerings WHERE reference_number = ?');
const getOfferingByMomoRef = db.prepare('SELECT * FROM offerings WHERE momo_reference_id = ?');

const updateOfferingStatus = db.prepare(`
  UPDATE offerings
  SET status = ?, financial_txn_id = ?, updated_at = CURRENT_TIMESTAMP
  WHERE reference_number = ?
`);

const updateDailySummaryCounters = db.prepare(`
  UPDATE daily_summaries
  SET success_count = success_count + ?, failed_count = failed_count + ?
  WHERE summary_date = ? AND category_code = ?
`);

const insertPendingQueue = db.prepare(`
  INSERT INTO pending_queue (reference_number, momo_reference_id, amount, phone_number, payer_message)
  VALUES (?, ?, ?, ?, ?)
`);

// --- Helper: format offering row for API response ---

function formatOffering(row) {
  return {
    referenceNumber: row.reference_number,
    momoReferenceId: row.momo_reference_id,
    category: row.category_code,
    categoryName: row.category_name,
    amount: row.amount,
    currency: row.currency,
    memberName: row.member_name,
    memberId: row.member_id,
    phone: maskPhone(row.phone_number),
    note: row.payer_message,
    status: row.status,
    financialTxnId: row.financial_txn_id,
    timestamp: row.created_at
  };
}

// --- Helper: compute daily_summaries delta for status transitions ---

function getStatusDelta(oldStatus, newStatus) {
  let successDelta = 0;
  let failedDelta = 0;

  // Remove contribution of old status
  if (oldStatus === 'SUCCESSFUL') successDelta -= 1;
  if (oldStatus === 'FAILED') failedDelta -= 1;

  // Add contribution of new status
  if (newStatus === 'SUCCESSFUL') successDelta += 1;
  if (newStatus === 'FAILED') failedDelta += 1;

  return { successDelta, failedDelta };
}

// --- Transactions ---

const createOfferingTxn = db.transaction((data) => {
  const refNumber = generateReference(data.category);
  const momoRefId = generateMomoReferenceId();
  const todayStr = getTodayDateString();

  insertOffering.run(
    refNumber, momoRefId, data.category, data.amount,
    data.phone, data.memberName || null, null, data.note || null
  );

  upsertDailySummary.run(todayStr, data.category, data.amount);

  return { refNumber, momoRefId };
});

const updateStatusTxn = db.transaction((referenceNumber, newStatus, financialTxnId) => {
  const offering = getOfferingByRef.get(referenceNumber);
  if (!offering) return null;

  const oldStatus = offering.status;
  if (oldStatus === newStatus) {
    return { offering, changed: false };
  }

  updateOfferingStatus.run(newStatus, financialTxnId || null, referenceNumber);

  // Extract date from created_at (format: "YYYY-MM-DD HH:MM:SS")
  const summaryDate = offering.created_at.split(' ')[0];
  const { successDelta, failedDelta } = getStatusDelta(oldStatus, newStatus);
  updateDailySummaryCounters.run(successDelta, failedDelta, summaryDate, offering.category_code);

  return { offering: { ...offering, status: newStatus, financial_txn_id: financialTxnId }, changed: true };
});

// ============================================================
// ROUTES
// ============================================================

/**
 * POST /api/offerings
 * Create a new offering and initiate MoMo payment request.
 * If MoMo API fails, offering is still stored as PENDING for retry/polling later.
 */
router.post('/', async (req, res) => {
  try {
    const { phone, amount, category, memberName, note } = req.body;

    // Validate required fields
    if (!phone) return res.status(400).json({ success: false, error: 'Phone number is required' });
    if (!amount) return res.status(400).json({ success: false, error: 'Amount is required' });
    if (!category) return res.status(400).json({ success: false, error: 'Category is required' });

    // Validate amount
    const parsedAmount = parseFloat(amount);
    if (isNaN(parsedAmount) || parsedAmount <= 0) {
      return res.status(400).json({ success: false, error: 'Amount must be a positive number' });
    }

    // Validate category exists
    const cat = getCategoryByCode.get(category);
    if (!cat) {
      const validCodes = getAllCategories.all().map(c => c.code);
      return res.status(400).json({
        success: false,
        error: `Invalid category '${category}'. Valid categories: ${validCodes.join(', ')}`
      });
    }

    // Create offering in a transaction (ref generation + insert + summary upsert)
    const { refNumber, momoRefId } = createOfferingTxn({
      phone, amount: parsedAmount, category, memberName, note
    });

    // Attempt MoMo payment request (non-blocking on failure)
    let momoError = null;
    try {
      await momoService.requestToPay({
        amount: parsedAmount,
        phone,
        referenceNumber: refNumber,
        message: note || `${cat.name} Offering`,
        momoReferenceId: momoRefId
      });
    } catch (err) {
      momoError = err.message;
      console.error(`MoMo requestToPay failed for ${refNumber}:`, err.message);

      // Queue for automatic retry
      try {
        insertPendingQueue.run(refNumber, momoRefId, parsedAmount, phone, note || `${cat.name} Offering`);
        console.log(`Queued ${refNumber} for automatic MoMo retry`);
      } catch (queueErr) {
        console.error(`Failed to queue ${refNumber} for retry:`, queueErr.message);
      }
    }

    res.status(202).json({
      success: true,
      referenceNumber: refNumber,
      momoReferenceId: momoRefId,
      status: 'PENDING',
      message: momoError
        ? `Offering recorded. MoMo unavailable — queued for automatic retry. Reference: ${refNumber}`
        : `Payment prompt sent to ${phone}. Awaiting PIN confirmation. Reference: ${refNumber}`,
      momoError: momoError || undefined
    });
  } catch (err) {
    console.error('POST /api/offerings error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * GET /api/offerings?date=YYYY-MM-DD
 * List all offerings for a date (default: today).
 */
router.get('/', (req, res) => {
  try {
    const date = req.query.date || getTodayDateString();
    if (!isValidDateString(date)) {
      return res.status(400).json({ success: false, error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const rows = getOfferingsByDate.all(date);
    const offerings = rows.map(formatOffering);

    const totalAmount = rows.reduce((sum, r) => sum + r.amount, 0);

    res.json({
      success: true,
      date,
      totalCount: rows.length,
      totalAmount: Math.round(totalAmount * 100) / 100,
      currency: 'GHS',
      offerings
    });
  } catch (err) {
    console.error('GET /api/offerings error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * GET /api/offerings/summary?date=YYYY-MM-DD
 * Per-category summary totals for a date.
 */
router.get('/summary', (req, res) => {
  try {
    const date = req.query.date || getTodayDateString();
    if (!isValidDateString(date)) {
      return res.status(400).json({ success: false, error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const categories = getSummaryByDate.all(date);

    const grandTotal = categories.reduce((sum, c) => sum + (c.total || 0), 0);
    const totalTransactions = categories.reduce((sum, c) => sum + c.count, 0);
    const totalSuccess = categories.reduce((sum, c) => sum + c.successCount, 0);
    const totalFailed = categories.reduce((sum, c) => sum + c.failedCount, 0);
    const successRate = totalTransactions > 0
      ? (totalSuccess / totalTransactions * 100).toFixed(1) + '%'
      : '0.0%';

    res.json({
      success: true,
      date,
      grandTotal: Math.round(grandTotal * 100) / 100,
      currency: 'GHS',
      totalTransactions,
      categories: categories.map(c => ({
        code: c.code,
        name: c.name,
        count: c.count,
        total: Math.round((c.total || 0) * 100) / 100
      })),
      successRate,
      failedCount: totalFailed
    });
  } catch (err) {
    console.error('GET /api/offerings/summary error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * GET /api/offerings/category/:code?date=YYYY-MM-DD
 * Offerings filtered by category.
 */
router.get('/category/:code', (req, res) => {
  try {
    const { code } = req.params;
    const date = req.query.date || getTodayDateString();

    if (!isValidDateString(date)) {
      return res.status(400).json({ success: false, error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    // Validate category exists
    const cat = getCategoryByCode.get(code);
    if (!cat) {
      return res.status(404).json({ success: false, error: `Category '${code}' not found` });
    }

    const rows = getOfferingsByDateAndCategory.all(code, date);
    const offerings = rows.map(formatOffering);
    const totalAmount = rows.reduce((sum, r) => sum + r.amount, 0);

    res.json({
      success: true,
      date,
      category: { code: cat.code, name: cat.name },
      totalCount: rows.length,
      totalAmount: Math.round(totalAmount * 100) / 100,
      currency: 'GHS',
      offerings
    });
  } catch (err) {
    console.error('GET /api/offerings/category error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * GET /api/offerings/:referenceNumber/status
 * Poll MoMo for the current status of an offering.
 * If status changed, updates DB and daily_summaries.
 */
router.get('/:referenceNumber/status', async (req, res) => {
  try {
    const { referenceNumber } = req.params;
    const offering = getOfferingByRef.get(referenceNumber);

    if (!offering) {
      return res.status(404).json({
        success: false,
        error: `Offering with reference '${referenceNumber}' not found`
      });
    }

    // If already in a terminal state, return without polling MoMo
    if (offering.status === 'SUCCESSFUL' || offering.status === 'FAILED') {
      return res.json({
        success: true,
        referenceNumber,
        status: offering.status,
        financialTxnId: offering.financial_txn_id,
        message: `Status: ${offering.status}`
      });
    }

    // Poll MoMo for current status
    let momoStatus = null;
    try {
      const momoResult = await momoService.checkStatus(offering.momo_reference_id);
      momoStatus = momoResult;
    } catch (err) {
      console.error(`MoMo checkStatus failed for ${referenceNumber}:`, err.message);
      return res.json({
        success: true,
        referenceNumber,
        status: offering.status,
        financialTxnId: offering.financial_txn_id,
        message: 'Unable to reach MoMo API. Current status from database.',
        momoError: err.message
      });
    }

    // If MoMo reports a different status, update the DB
    const newStatus = momoStatus.status;
    const financialTxnId = momoStatus.financialTransactionId || null;

    if (newStatus && newStatus !== offering.status) {
      updateStatusTxn(referenceNumber, newStatus, financialTxnId);
    }

    res.json({
      success: true,
      referenceNumber,
      status: newStatus || offering.status,
      financialTxnId: financialTxnId || offering.financial_txn_id,
      message: `Status: ${newStatus || offering.status}`
    });
  } catch (err) {
    console.error('GET /api/offerings/:ref/status error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * PUT /api/offerings/:referenceNumber/status
 * Update offering status and daily_summaries.
 */
router.put('/:referenceNumber/status', (req, res) => {
  try {
    const { referenceNumber } = req.params;
    const { status, financialTxnId } = req.body;

    // Validate status
    const validStatuses = ['PENDING', 'SUCCESSFUL', 'FAILED'];
    if (!status || !validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        error: `Invalid status. Must be one of: ${validStatuses.join(', ')}`
      });
    }

    const result = updateStatusTxn(referenceNumber, status, financialTxnId);

    if (!result) {
      return res.status(404).json({
        success: false,
        error: `Offering with reference '${referenceNumber}' not found`
      });
    }

    res.json({
      success: true,
      referenceNumber,
      status,
      financialTxnId: financialTxnId || null,
      changed: result.changed,
      message: result.changed
        ? `Status updated to ${status}`
        : `Status already ${status}, no change`
    });
  } catch (err) {
    console.error('PUT /api/offerings/:ref/status error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

/**
 * GET /api/offerings/export?date=YYYY-MM-DD&format=csv
 * Export offerings as CSV download.
 */
router.get('/export', (req, res) => {
  try {
    const date = req.query.date || getTodayDateString();
    const format = req.query.format;

    if (!isValidDateString(date)) {
      return res.status(400).json({ success: false, error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    if (format !== 'csv') {
      return res.status(400).json({ success: false, error: 'Only CSV format is supported. Use format=csv' });
    }

    const rows = getOfferingsByDate.all(date);
    const csv = offeringsToCSV(rows);

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="offerings-${date}.csv"`);
    res.send(csv);
  } catch (err) {
    console.error('GET /api/offerings/export error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

module.exports = router;
