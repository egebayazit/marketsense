PRAGMA foreign_keys=ON;

-- Daily price bars
CREATE TABLE IF NOT EXISTS stocks (
  ticker  TEXT NOT NULL,
  date    TEXT NOT NULL,  -- ISO 'YYYY-MM-DD'
  open    REAL, high REAL, low REAL,
  close   REAL NOT NULL,
  volume  INTEGER DEFAULT 0 CHECK (volume >= 0),
  PRIMARY KEY (ticker, date),
  CHECK (low IS NULL OR high IS NULL OR low <= high),
  CHECK (close >= 0)
);

-- Fast “most recent” lookups
CREATE INDEX IF NOT EXISTS idx_stocks_ticker_date_desc
  ON stocks(ticker, date DESC);

-- Minimal news table
CREATE TABLE IF NOT EXISTS news (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  headline     TEXT NOT NULL,
  published_at TEXT NOT NULL,  -- ISO 8601
  source       TEXT,
  url          TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_news_published_at
  ON news(published_at DESC);
