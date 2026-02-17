const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

// Ensure data directory exists
const dataDir = path.join(__dirname, '..', 'data');
fs.mkdirSync(dataDir, { recursive: true });

const dbPath = path.join(dataDir, 'offerings.db');
const db = new Database(dbPath);

// Enable WAL mode for better read concurrency
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

function initializeDatabase() {
  // Create tables
  db.exec(`
    CREATE TABLE IF NOT EXISTS categories (
      code        TEXT PRIMARY KEY,
      name        TEXT NOT NULL,
      description TEXT
    );

    CREATE TABLE IF NOT EXISTS offerings (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      reference_number    TEXT UNIQUE NOT NULL,
      momo_reference_id   TEXT UNIQUE NOT NULL,
      financial_txn_id    TEXT,
      category_code       TEXT NOT NULL REFERENCES categories(code),
      amount              REAL NOT NULL,
      currency            TEXT DEFAULT 'GHS',
      phone_number        TEXT NOT NULL,
      member_name         TEXT,
      member_id           TEXT,
      payer_message       TEXT,
      status              TEXT DEFAULT 'PENDING',
      created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS daily_summaries (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      summary_date    DATE NOT NULL,
      category_code   TEXT NOT NULL REFERENCES categories(code),
      total_count     INTEGER DEFAULT 0,
      total_amount    REAL DEFAULT 0.0,
      success_count   INTEGER DEFAULT 0,
      failed_count    INTEGER DEFAULT 0,
      UNIQUE(summary_date, category_code)
    );

    CREATE TABLE IF NOT EXISTS pending_queue (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      reference_number  TEXT NOT NULL REFERENCES offerings(reference_number),
      momo_reference_id TEXT NOT NULL,
      amount            REAL NOT NULL,
      phone_number      TEXT NOT NULL,
      payer_message     TEXT,
      retry_count       INTEGER DEFAULT 0,
      last_attempt_at   DATETIME,
      created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_offerings_date ON offerings(created_at);
    CREATE INDEX IF NOT EXISTS idx_offerings_category ON offerings(category_code);
    CREATE INDEX IF NOT EXISTS idx_offerings_status ON offerings(status);
    CREATE INDEX IF NOT EXISTS idx_offerings_ref ON offerings(reference_number);
    CREATE INDEX IF NOT EXISTS idx_pending_queue_retry ON pending_queue(retry_count, last_attempt_at);
  `);

  // Seed categories (idempotent)
  const insertCategory = db.prepare(
    'INSERT OR IGNORE INTO categories (code, name, description) VALUES (?, ?, ?)'
  );

  const seedCategories = db.transaction(() => {
    insertCategory.run('TITHE',  'Tithes',            '10% income offering from members');
    insertCategory.run('OFFR',   'General Offering',   'Regular Sunday/service offering');
    insertCategory.run('SEED',   'Seed Offering',      'Special faith-based seed offerings');
    insertCategory.run('PLEDG',  'Pledge Redemption',  'Fulfillment of previously made pledges');
    insertCategory.run('BUILD',  'Building Fund',      'Contributions toward church construction');
    insertCategory.run('MISN',   'Missions',           'Missionary and outreach fund');
    insertCategory.run('WELFR',  'Welfare',            'Benevolence and member welfare support');
    insertCategory.run('YOUTH',  'Youth Ministry',     'Youth department specific offerings');
    insertCategory.run('THANKS', 'Thanksgiving',       'Special thanksgiving offerings');
    insertCategory.run('FIRST',  'First Fruit',        'First fruit / first salary offerings');
  });

  seedCategories();

  console.log('Database initialized: tables created, categories seeded.');
}

// Initialize immediately so tables exist before any module prepares statements
initializeDatabase();

module.exports = { db, initializeDatabase };
