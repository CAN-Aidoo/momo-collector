/**
 * Mask a phone number for display: "0241234567" → "024***4567"
 * Shows first 3, masks middle with ***, shows last 4.
 */
function maskPhone(phone) {
  if (!phone || phone.length < 7) return phone || '';
  return phone.slice(0, 3) + '***' + phone.slice(-4);
}

/**
 * Get today's date as YYYY-MM-DD in the server's local timezone.
 */
function getTodayDateString() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Validate a date string matches YYYY-MM-DD format.
 */
function isValidDateString(dateStr) {
  return /^\d{4}-\d{2}-\d{2}$/.test(dateStr);
}

/**
 * Convert an array of offering rows into a CSV string.
 * Phone numbers are masked in the output.
 */
function offeringsToCSV(offerings) {
  const headers = [
    'Reference Number',
    'Category',
    'Category Name',
    'Amount (GHS)',
    'Phone',
    'Member Name',
    'Member ID',
    'Note',
    'Status',
    'Financial Txn ID',
    'Timestamp'
  ];

  const rows = offerings.map(o => [
    o.reference_number,
    o.category_code,
    o.category_name || '',
    o.amount,
    maskPhone(o.phone_number),
    o.member_name || '',
    o.member_id || '',
    o.payer_message || '',
    o.status,
    o.financial_txn_id || '',
    o.created_at
  ]);

  const escapeField = (field) => {
    const str = String(field);
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  const lines = [
    headers.join(','),
    ...rows.map(row => row.map(escapeField).join(','))
  ];

  return lines.join('\n');
}

module.exports = { maskPhone, getTodayDateString, isValidDateString, offeringsToCSV };
