CREATE TABLE IF NOT EXISTS snapshot_cache (
  window_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS usdm_daily_stats (
  snapshot_day TEXT NOT NULL,
  symbol TEXT NOT NULL,
  base_asset TEXT NOT NULL,
  quote_asset TEXT NOT NULL,
  open_price REAL NOT NULL,
  high_price REAL NOT NULL,
  low_price REAL NOT NULL,
  last_price REAL NOT NULL,
  captured_at TEXT NOT NULL,
  PRIMARY KEY (snapshot_day, symbol)
);

CREATE INDEX IF NOT EXISTS idx_usdm_daily_stats_symbol_day
ON usdm_daily_stats (symbol, snapshot_day);
