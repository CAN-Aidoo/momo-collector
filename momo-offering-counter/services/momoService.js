const crypto = require('crypto');

const MOMO_BASE = process.env.MOMO_BASE_URL;
const SUBSCRIPTION_KEY = process.env.MOMO_SUBSCRIPTION_KEY;
const USER_ID = process.env.MOMO_USER_ID;
const API_KEY = process.env.MOMO_API_KEY;
const ENVIRONMENT = process.env.MOMO_ENVIRONMENT || 'sandbox';
const CALLBACK_URL = process.env.MOMO_CALLBACK_URL;
const DEMO_MODE = process.env.DEMO_MODE === 'true';

// Token cache
let cachedToken = null;
let tokenExpiresAt = 0;

// Demo mode: in-memory simulated status store
const demoStatuses = new Map();

/**
 * Get an OAuth 2.0 Bearer token from MoMo Collections API.
 * Caches the token for 3500 seconds (expires at 3600).
 * In DEMO_MODE, returns a fake token instantly.
 */
async function getToken() {
  if (DEMO_MODE) {
    return 'demo-token-' + Date.now();
  }

  const now = Date.now();
  if (cachedToken && now < tokenExpiresAt) {
    return cachedToken;
  }

  const credentials = Buffer.from(`${USER_ID}:${API_KEY}`).toString('base64');

  const res = await fetch(`${MOMO_BASE}/collection/token/`, {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${credentials}`,
      'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY
    }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`MoMo token request failed (${res.status}): ${text}`);
  }

  const data = await res.json();
  cachedToken = data.access_token;
  tokenExpiresAt = now + 3500 * 1000; // Cache for 3500s (token valid for 3600s)

  return cachedToken;
}

/**
 * Convert local Ghana phone format to MSISDN.
 * "0241234567" → "233241234567"
 */
function toMSISDN(phone) {
  if (phone.startsWith('0')) {
    return '233' + phone.slice(1);
  }
  return phone;
}

/**
 * Initiate a MoMo Request to Pay.
 * @param {string} momoReferenceId - Pre-generated UUID to use as X-Reference-Id
 * Returns { referenceId } — the X-Reference-Id UUID used to track this request.
 *
 * In DEMO_MODE: skips real API, schedules a simulated status after 3s
 * (90% SUCCESSFUL, 10% FAILED).
 */
async function requestToPay({ amount, phone, referenceNumber, message, momoReferenceId }) {
  const referenceId = momoReferenceId || crypto.randomUUID();

  if (DEMO_MODE) {
    // Store initial PENDING status
    demoStatuses.set(referenceId, { status: 'PENDING', referenceNumber });

    // Schedule simulated result after 3 seconds
    setTimeout(() => {
      const simulatedStatus = Math.random() < 0.9 ? 'SUCCESSFUL' : 'FAILED';
      const financialTxnId = simulatedStatus === 'SUCCESSFUL'
        ? 'FT' + Date.now().toString(36).toUpperCase()
        : null;

      demoStatuses.set(referenceId, {
        status: simulatedStatus,
        referenceNumber,
        financialTransactionId: financialTxnId
      });

      // Also update the database via the updateStatusTxn pattern
      try {
        const { db } = require('../db/database');
        const offering = db.prepare('SELECT * FROM offerings WHERE momo_reference_id = ?').get(referenceId);
        if (offering && offering.status === 'PENDING') {
          db.prepare('UPDATE offerings SET status = ?, financial_txn_id = ?, updated_at = CURRENT_TIMESTAMP WHERE momo_reference_id = ?')
            .run(simulatedStatus, financialTxnId, referenceId);

          // Update daily_summaries
          const summaryDate = offering.created_at.split(' ')[0];
          const successDelta = simulatedStatus === 'SUCCESSFUL' ? 1 : 0;
          const failedDelta = simulatedStatus === 'FAILED' ? 1 : 0;
          db.prepare('UPDATE daily_summaries SET success_count = success_count + ?, failed_count = failed_count + ? WHERE summary_date = ? AND category_code = ?')
            .run(successDelta, failedDelta, summaryDate, offering.category_code);
        }
      } catch (err) {
        console.error('Demo: failed to update DB status:', err.message);
      }

      console.log(`Demo: ${referenceNumber} → ${simulatedStatus}`);
    }, 3000);

    console.log(`Demo: requestToPay accepted for ${referenceNumber} (will resolve in 3s)`);
    return { referenceId };
  }

  const token = await getToken();
  const msisdn = toMSISDN(phone);

  const res = await fetch(`${MOMO_BASE}/collection/v1_0/requesttopay`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'X-Reference-Id': referenceId,
      'X-Target-Environment': ENVIRONMENT,
      'X-Callback-Url': CALLBACK_URL,
      'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      amount: String(amount),
      currency: 'GHS',
      externalId: referenceNumber,
      payer: { partyIdType: 'MSISDN', partyId: msisdn },
      payerMessage: message || 'Church Offering',
      payeeNote: referenceNumber
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`MoMo requestToPay failed (${res.status}): ${text}`);
  }

  // MoMo returns 202 Accepted with empty body on success
  return { referenceId };
}

/**
 * Check the status of a MoMo Request to Pay.
 * Returns the transaction object with status: SUCCESSFUL, FAILED, or PENDING.
 *
 * In DEMO_MODE: returns the simulated status from the in-memory store,
 * or falls back to checking the database directly.
 */
async function checkStatus(referenceId) {
  if (DEMO_MODE) {
    // Check in-memory demo store first
    const demoEntry = demoStatuses.get(referenceId);
    if (demoEntry) {
      return {
        status: demoEntry.status,
        financialTransactionId: demoEntry.financialTransactionId || null,
        externalId: demoEntry.referenceNumber
      };
    }
    // Fallback: check DB directly (for seeded data)
    const { db } = require('../db/database');
    const offering = db.prepare('SELECT * FROM offerings WHERE momo_reference_id = ?').get(referenceId);
    if (offering) {
      return {
        status: offering.status,
        financialTransactionId: offering.financial_txn_id || null,
        externalId: offering.reference_number
      };
    }
    return { status: 'PENDING', financialTransactionId: null };
  }

  const token = await getToken();

  const res = await fetch(
    `${MOMO_BASE}/collection/v1_0/requesttopay/${referenceId}`,
    {
      headers: {
        'Authorization': `Bearer ${token}`,
        'X-Target-Environment': ENVIRONMENT,
        'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY
      }
    }
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`MoMo checkStatus failed (${res.status}): ${text}`);
  }

  return res.json();
}

module.exports = { getToken, requestToPay, checkStatus, DEMO_MODE };
