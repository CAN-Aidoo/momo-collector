const express = require('express');
const crypto = require('crypto');
const router = express.Router();

const { db } = require('../db/database');
const { generateReference, generateMomoReferenceId } = require('../services/referenceGen');
const { getTodayDateString } = require('../utils/helpers');

// --- Ghanaian names pool ---
const FIRST_NAMES = [
  'Kwame', 'Ama', 'Kofi', 'Akua', 'Yaw', 'Abena', 'Kwesi', 'Efua',
  'Kwabena', 'Adwoa', 'Kojo', 'Afia', 'Nana', 'Akosua', 'Kwaku', 'Esi',
  'Papa', 'Maame', 'Fiifi', 'Adjoa', 'Nii', 'Naana', 'Mensah', 'Aba',
  'Yoofi', 'Araba', 'Ekow', 'Baaba', 'Kobby', 'Dede'
];

const LAST_NAMES = [
  'Mensah', 'Asante', 'Owusu', 'Osei', 'Acheampong', 'Boateng', 'Adjei',
  'Appiah', 'Amoah', 'Danquah', 'Annan', 'Ofori', 'Ankrah', 'Darko',
  'Badu', 'Gyamfi', 'Frimpong', 'Agyeman', 'Sarpong', 'Konadu',
  'Amponsah', 'Nkrumah', 'Addai', 'Bonsu', 'Oppong', 'Ansah',
  'Tetteh', 'Quaye', 'Lamptey', 'Ampofo'
];

// MTN Ghana prefixes
const MTN_PREFIXES = ['024', '054', '055', '059'];

// --- Helpers ---

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomAmount(min, max) {
  // Round to nearest 0.50
  const raw = min + Math.random() * (max - min);
  return Math.round(raw * 2) / 2;
}

function randomPhone() {
  const prefix = randomChoice(MTN_PREFIXES);
  const suffix = String(randomInt(1000000, 9999999));
  return prefix + suffix;
}

function randomName() {
  return randomChoice(FIRST_NAMES) + ' ' + randomChoice(LAST_NAMES);
}

function randomStatus() {
  return Math.random() < 0.9 ? 'SUCCESSFUL' : 'FAILED';
}

// --- Prepared statements ---

const insertOffering = db.prepare(`
  INSERT INTO offerings (reference_number, momo_reference_id, category_code, amount, phone_number, member_name, member_id, payer_message, status, financial_txn_id)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const upsertDailySummary = db.prepare(`
  INSERT INTO daily_summaries (summary_date, category_code, total_count, total_amount, success_count, failed_count)
  VALUES (?, ?, 1, ?, ?, ?)
  ON CONFLICT(summary_date, category_code)
  DO UPDATE SET
    total_count = total_count + 1,
    total_amount = total_amount + excluded.total_amount,
    success_count = success_count + excluded.success_count,
    failed_count = failed_count + excluded.failed_count
`);

const deleteAllOfferings = db.prepare('DELETE FROM offerings');
const deleteAllSummaries = db.prepare('DELETE FROM daily_summaries');
const deleteAllPendingQueue = db.prepare('DELETE FROM pending_queue');

// --- Category seed config: { code, minCount, maxCount, minAmount, maxAmount } ---

const SEED_CONFIG = [
  { code: 'TITHE',  minCount: 30, maxCount: 40, minAmount: 20,  maxAmount: 500  },
  { code: 'OFFR',   minCount: 25, maxCount: 35, minAmount: 5,   maxAmount: 100  },
  { code: 'SEED',   minCount: 8,  maxCount: 12, minAmount: 50,  maxAmount: 1000 },
  { code: 'BUILD',  minCount: 5,  maxCount: 10, minAmount: 100, maxAmount: 2000 },
  { code: 'PLEDG',  minCount: 3,  maxCount: 8,  minAmount: 50,  maxAmount: 500  },
  { code: 'MISN',   minCount: 3,  maxCount: 8,  minAmount: 20,  maxAmount: 300  },
  { code: 'WELFR',  minCount: 3,  maxCount: 8,  minAmount: 10,  maxAmount: 200  },
  { code: 'YOUTH',  minCount: 3,  maxCount: 8,  minAmount: 5,   maxAmount: 100  },
  { code: 'THANKS', minCount: 3,  maxCount: 8,  minAmount: 50,  maxAmount: 500  },
  { code: 'FIRST',  minCount: 3,  maxCount: 8,  minAmount: 100, maxAmount: 1000 },
];

// Messages per category
const CATEGORY_MESSAGES = {
  TITHE:  'Tithes Offering',
  OFFR:   'General Offering',
  SEED:   'Seed Offering',
  BUILD:  'Building Fund',
  PLEDG:  'Pledge Redemption',
  MISN:   'Missions Offering',
  WELFR:  'Welfare Offering',
  YOUTH:  'Youth Ministry',
  THANKS: 'Thanksgiving Offering',
  FIRST:  'First Fruit Offering',
};

/**
 * POST /api/demo/seed
 * Generate realistic test data for today.
 */
router.post('/seed', (req, res) => {
  try {
    const todayStr = getTodayDateString();
    let totalSeeded = 0;

    const seedAll = db.transaction(() => {
      for (const cfg of SEED_CONFIG) {
        const count = randomInt(cfg.minCount, cfg.maxCount);

        for (let i = 0; i < count; i++) {
          const refNumber = generateReference(cfg.code);
          const momoRefId = generateMomoReferenceId();
          const amount = randomAmount(cfg.minAmount, cfg.maxAmount);
          const phone = randomPhone();
          const name = randomName();
          const status = randomStatus();
          const financialTxnId = status === 'SUCCESSFUL'
            ? 'FT' + crypto.randomBytes(6).toString('hex').toUpperCase()
            : null;
          const message = CATEGORY_MESSAGES[cfg.code] || 'Church Offering';

          insertOffering.run(
            refNumber, momoRefId, cfg.code, amount, phone,
            name, null, message, status, financialTxnId
          );

          upsertDailySummary.run(
            todayStr, cfg.code, amount,
            status === 'SUCCESSFUL' ? 1 : 0,
            status === 'FAILED' ? 1 : 0
          );

          totalSeeded++;
        }
      }
    });

    seedAll();

    console.log(`Demo: seeded ${totalSeeded} offerings for ${todayStr}`);

    res.json({
      success: true,
      message: `Seeded ${totalSeeded} demo offerings for today`,
      count: totalSeeded,
      date: todayStr
    });
  } catch (err) {
    console.error('POST /api/demo/seed error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * POST /api/demo/clear
 * Delete all offerings and reset daily_summaries.
 */
router.post('/clear', (req, res) => {
  try {
    const clearAll = db.transaction(() => {
      deleteAllPendingQueue.run();
      deleteAllOfferings.run();
      deleteAllSummaries.run();
    });

    clearAll();

    console.log('Demo: cleared all offerings, summaries, and pending queue');

    res.json({
      success: true,
      message: 'All offerings, summaries, and pending queue cleared'
    });
  } catch (err) {
    console.error('POST /api/demo/clear error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
