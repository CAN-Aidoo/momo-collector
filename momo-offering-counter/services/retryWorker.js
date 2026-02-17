const { db } = require('../db/database');
const momoService = require('./momoService');

const RETRY_INTERVAL_MS = 60000;   // Run every 60 seconds
const MAX_RETRIES = 5;             // Give up after 5 attempts
const COOLDOWN_MINUTES = 2;        // Wait 2 min between retries per item
const BATCH_SIZE = 10;             // Process up to 10 items per tick

// Prepared statements
const getPendingItems = db.prepare(`
  SELECT id, reference_number, momo_reference_id, amount, phone_number, payer_message
  FROM pending_queue
  WHERE retry_count < ?
    AND (last_attempt_at IS NULL OR last_attempt_at < datetime('now', '-${COOLDOWN_MINUTES} minutes'))
  ORDER BY created_at ASC
  LIMIT ?
`);

const deleteQueueItem = db.prepare(
  'DELETE FROM pending_queue WHERE id = ?'
);

const incrementRetry = db.prepare(`
  UPDATE pending_queue
  SET retry_count = retry_count + 1,
      last_attempt_at = CURRENT_TIMESTAMP
  WHERE id = ?
`);

/**
 * Process one tick of the retry worker.
 * Queries pending_queue for eligible items and retries MoMo requestToPay.
 */
async function processPendingQueue() {
  const items = getPendingItems.all(MAX_RETRIES, BATCH_SIZE);

  if (items.length === 0) return;

  let succeeded = 0;
  let failed = 0;

  for (const item of items) {
    try {
      await momoService.requestToPay({
        amount: item.amount,
        phone: item.phone_number,
        referenceNumber: item.reference_number,
        message: item.payer_message || 'Church Offering',
        momoReferenceId: item.momo_reference_id
      });

      // MoMo accepted the request (202) — remove from queue
      // Callback or polling will handle final SUCCESSFUL/FAILED status
      deleteQueueItem.run(item.id);
      succeeded++;
    } catch (err) {
      // MoMo still unreachable — bump retry count
      incrementRetry.run(item.id);
      failed++;
      console.error(
        `Retry failed for ${item.reference_number} (attempt ${item.retry_count + 1}/${MAX_RETRIES}):`,
        err.message
      );
    }
  }

  console.log(
    `Retry worker: processed ${items.length} item(s) — ${succeeded} succeeded, ${failed} failed`
  );
}

/**
 * Start the retry worker on a fixed interval.
 * Call once from server.js after app.listen().
 */
function startRetryWorker() {
  console.log(
    `Retry worker started: checking every ${RETRY_INTERVAL_MS / 1000}s, ` +
    `max ${MAX_RETRIES} retries, ${COOLDOWN_MINUTES}min cooldown`
  );

  // Run once immediately to clear any backlog from server downtime
  processPendingQueue().catch(err =>
    console.error('Retry worker initial run error:', err.message)
  );

  setInterval(() => {
    processPendingQueue().catch(err =>
      console.error('Retry worker error:', err.message)
    );
  }, RETRY_INTERVAL_MS);
}

module.exports = { startRetryWorker };
