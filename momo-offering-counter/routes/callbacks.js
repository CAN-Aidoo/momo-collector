const express = require('express');
const router = express.Router();

const { db } = require('../db/database');

// --- Prepared statements ---

const getOfferingByRef = db.prepare('SELECT * FROM offerings WHERE reference_number = ?');

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

// --- Helper: compute daily_summaries delta for status transitions ---

function getStatusDelta(oldStatus, newStatus) {
  let successDelta = 0;
  let failedDelta = 0;

  if (oldStatus === 'SUCCESSFUL') successDelta -= 1;
  if (oldStatus === 'FAILED') failedDelta -= 1;
  if (newStatus === 'SUCCESSFUL') successDelta += 1;
  if (newStatus === 'FAILED') failedDelta += 1;

  return { successDelta, failedDelta };
}

const updateStatusTxn = db.transaction((referenceNumber, newStatus, financialTxnId) => {
  const offering = getOfferingByRef.get(referenceNumber);
  if (!offering) return null;

  const oldStatus = offering.status;
  if (oldStatus === newStatus) {
    return { offering, changed: false };
  }

  updateOfferingStatus.run(newStatus, financialTxnId || null, referenceNumber);

  const summaryDate = offering.created_at.split(' ')[0];
  const { successDelta, failedDelta } = getStatusDelta(oldStatus, newStatus);
  updateDailySummaryCounters.run(successDelta, failedDelta, summaryDate, offering.category_code);

  return { offering: { ...offering, status: newStatus, financial_txn_id: financialTxnId }, changed: true };
});

/**
 * POST /api/callbacks/momo
 * Webhook endpoint for MTN MoMo status updates.
 * Receives: { externalId, status, financialTransactionId }
 * The externalId is our reference number (e.g., TITHE-2026-0211-001).
 */
router.post('/momo', (req, res) => {
  try {
    const { externalId, status, financialTransactionId } = req.body;

    console.log(`MoMo callback received: externalId=${externalId}, status=${status}, txnId=${financialTransactionId}`);

    if (!externalId || !status) {
      return res.status(400).json({ success: false, error: 'Missing externalId or status' });
    }

    // Validate status value
    const validStatuses = ['SUCCESSFUL', 'FAILED', 'PENDING'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ success: false, error: `Invalid status: ${status}` });
    }

    const result = updateStatusTxn(externalId, status, financialTransactionId);

    if (!result) {
      console.error(`MoMo callback: offering '${externalId}' not found in database`);
      return res.status(404).json({ success: false, error: 'Offering not found' });
    }

    if (result.changed) {
      console.log(`MoMo callback: ${externalId} updated to ${status}`);
    }

    res.json({ success: true });
  } catch (err) {
    console.error('POST /api/callbacks/momo error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

module.exports = router;
