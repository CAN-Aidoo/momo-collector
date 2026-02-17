const crypto = require('crypto');
const { db } = require('../db/database');

const countTodayByCategory = db.prepare(`
  SELECT COUNT(*) as count FROM offerings
  WHERE category_code = ? AND DATE(created_at) = ?
`);

/**
 * Generate a structured reference number: {CATEGORY}-{YYYY}-{MMDD}-{SEQ}
 * Sequence is zero-padded to 3 digits, based on count of today's offerings in that category.
 */
function generateReference(categoryCode) {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const todayStr = `${year}-${month}-${day}`;

  const row = countTodayByCategory.get(categoryCode, todayStr);
  const seq = String((row.count || 0) + 1).padStart(3, '0');

  return `${categoryCode}-${year}-${month}${day}-${seq}`;
}

/**
 * Generate a UUID for the MoMo reference ID.
 */
function generateMomoReferenceId() {
  return crypto.randomUUID();
}

module.exports = { generateReference, generateMomoReferenceId };
