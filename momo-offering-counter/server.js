require('dotenv').config();

const express = require('express');
const path = require('path');
const offeringsRouter = require('./routes/offerings');
const callbacksRouter = require('./routes/callbacks');
const demoRouter = require('./routes/demo');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Routes
app.use('/api/offerings', offeringsRouter);
app.use('/api/callbacks', callbacksRouter);
app.use('/api/demo', demoRouter);

// App config endpoint — exposes safe settings to frontend
const DEMO_MODE = process.env.DEMO_MODE === 'true';
app.get('/api/config', (req, res) => {
  res.json({ demoMode: DEMO_MODE });
});

// Categories endpoint
const { db } = require('./db/database');
const momoService = require('./services/momoService');
const getAllCategoriesFull = db.prepare('SELECT code, name, description FROM categories ORDER BY name');
const getQueueSize = db.prepare('SELECT COUNT(*) as size FROM pending_queue WHERE retry_count < 5');

app.get('/api/categories', (req, res) => {
  try {
    const categories = getAllCategoriesFull.all();
    res.json({ success: true, categories });
  } catch (err) {
    console.error('GET /api/categories error:', err.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Health check — enhanced with MoMo connectivity + retry queue size
app.get('/api/health', async (req, res) => {
  let momoReachable = false;
  try {
    await Promise.race([
      momoService.getToken(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
    ]);
    momoReachable = true;
  } catch {
    // MoMo unreachable — that's okay, we report it
  }

  const queueSize = getQueueSize.get().size;

  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    momoReachable,
    queueSize
  });
});

// Database initializes automatically when db/database.js is first required
app.listen(PORT, () => {
  console.log(`MoMo Offering Counter running on http://localhost:${PORT}${DEMO_MODE ? ' [DEMO MODE]' : ''}`);

  // Start the retry worker for queued MoMo requests
  const { startRetryWorker } = require('./services/retryWorker');
  startRetryWorker();
});
