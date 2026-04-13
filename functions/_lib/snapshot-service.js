const QUOTE_ASSET = "USDT";
const DAY_MS = 86_400_000;
const MEMORY_CACHE_MS = 60_000;
const INGEST_FRESHNESS_MS = 90 * 60_000;

const WINDOW_CONFIG = {
  "1d": {
    label: "1天",
    days: 1,
  },
  "3d": {
    label: "3天",
    days: 3,
  },
  "7d": {
    label: "7天",
    days: 7,
  },
};

const runtimeCache = new Map();
let schemaPromise = null;

export function isSupportedWindow(windowKey) {
  return Object.prototype.hasOwnProperty.call(WINDOW_CONFIG, windowKey);
}

export async function getSnapshot(env, windowKey, options = {}) {
  if (!isSupportedWindow(windowKey)) {
    throw new Error(`Unsupported window: ${windowKey}`);
  }

  if (!hasD1(env)) {
    throw new Error("D1 is not configured");
  }

  const force = options.force === true;
  const now = Date.now();
  const cacheKey = `snapshot:${windowKey}`;
  const cached = runtimeCache.get(cacheKey);

  if (!force && cached?.value && cached.memoryExpiresAt > now) {
    const repairedCachedValue = await maybeRepairUsdmSnapshot(env, cached.value, windowKey);
    runtimeCache.set(cacheKey, {
      value: repairedCachedValue,
      memoryExpiresAt: cached.memoryExpiresAt,
      snapshotExpiresAt: cached.snapshotExpiresAt,
    });

    return decorateSnapshot(repairedCachedValue, {
      cacheStatus: "memory-hit",
      storage: "d1",
      hasD1: true,
      expiresAt: cached.snapshotExpiresAt,
      isStale: cached.snapshotExpiresAt <= now,
    });
  }

  const dbEntry = await readSnapshotCache(env, windowKey);
  if (!dbEntry?.value) {
    throw new Error("No ingested snapshot available yet");
  }

  const repairedValue = await maybeRepairUsdmSnapshot(env, dbEntry.value, windowKey);

  runtimeCache.set(cacheKey, {
    value: repairedValue,
    memoryExpiresAt: now + MEMORY_CACHE_MS,
    snapshotExpiresAt: dbEntry.expiresAt,
  });

  return decorateSnapshot(repairedValue, {
    cacheStatus: force ? "reload" : "d1-hit",
    storage: "d1",
    hasD1: true,
    expiresAt: dbEntry.expiresAt,
    isStale: dbEntry.expiresAt <= now,
  });
}

export async function ingestSnapshotBundle(env, input) {
  if (!hasD1(env)) {
    throw new Error("D1 is not configured");
  }

  await ensureDbSchema(env);
  const normalized = normalizeIngestPayload(input);

  await upsertUsdmDailyStats(env, normalized.usdmSymbols, normalized.usdm24hMap, normalized.capturedAtMs);
  const historyState = await loadUsdmHistoryRows(env, WINDOW_CONFIG["7d"].days, normalized.capturedAtMs);

  const windows = {};
  for (const windowKey of Object.keys(WINDOW_CONFIG)) {
    const snapshot = buildSnapshotFromIngest(normalized, windowKey, historyState);
    const snapshotExpiresAt = normalized.capturedAtMs + INGEST_FRESHNESS_MS;

    await writeSnapshotCache(env, windowKey, snapshot, snapshotExpiresAt);
    runtimeCache.set(`snapshot:${windowKey}`, {
      value: snapshot,
      memoryExpiresAt: Date.now() + MEMORY_CACHE_MS,
      snapshotExpiresAt,
    });

    windows[windowKey] = {
      generatedAt: snapshot.generatedAt,
      counts: snapshot.counts,
    };
  }

  return {
    capturedAt: normalized.capturedAt,
    freshnessTtlMinutes: Math.round(INGEST_FRESHNESS_MS / 60_000),
    windows,
  };
}

function decorateSnapshot(snapshot, backend) {
  return {
    ...snapshot,
    backend: {
      ...backend,
      servedAt: new Date().toISOString(),
    },
  };
}

function normalizeIngestPayload(input) {
  if (!input || typeof input !== "object") {
    throw new Error("Ingest payload must be a JSON object");
  }

  const capturedAtMs = parseTimestamp(input.capturedAt) ?? Date.now();
  const capturedAt = new Date(capturedAtMs).toISOString();
  const source = typeof input.source === "string" && input.source.trim()
    ? input.source.trim().slice(0, 80)
    : "external-ingest";

  const spot24h = normalizeTickerArray(input.spot24h, "spot24h");
  const usdm24h = normalizeTickerArray(input.usdm24h, "usdm24h");
  const spotRolling = input.spotRolling && typeof input.spotRolling === "object" ? input.spotRolling : {};
  const spotRolling3d = normalizeTickerArray(spotRolling["3d"], "spotRolling.3d");
  const spotRolling7d = normalizeTickerArray(spotRolling["7d"], "spotRolling.7d");

  return {
    capturedAt,
    capturedAtMs,
    source,
    spotSymbols: createSymbolMetasFromTickers(spot24h, {
      market: "spot",
      segment: "spot",
      segmentLabel: "现货",
    }),
    usdmSymbols: createSymbolMetasFromTickers(usdm24h, {
      market: "perpetual",
      segment: "usdm",
      segmentLabel: "U本位永续",
    }),
    spot24hMap: indexBySymbol(spot24h),
    usdm24hMap: indexBySymbol(usdm24h),
    spotRollingMaps: {
      "1d": indexBySymbol(spot24h),
      "3d": indexBySymbol(spotRolling3d),
      "7d": indexBySymbol(spotRolling7d),
    },
  };
}

function normalizeTickerArray(value, label) {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }

  return value.filter((item) => item?.symbol && isUsdtTradingSymbol(item.symbol));
}

function parseTimestamp(value) {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function createSymbolMetasFromTickers(items, meta) {
  const symbols = new Set();

  for (const item of items) {
    if (isUsdtTradingSymbol(item?.symbol)) {
      symbols.add(item.symbol);
    }
  }

  return Array.from(symbols)
    .sort()
    .map((symbol) =>
      createSymbolMeta({
        ...meta,
        symbol,
      }),
    );
}

function indexBySymbol(items) {
  const result = new Map();

  for (const item of items) {
    if (item?.symbol && isUsdtTradingSymbol(item.symbol)) {
      result.set(item.symbol, item);
    }
  }

  return result;
}

function buildSnapshotFromIngest(normalized, windowKey, historyState) {
  const effectiveHistoryState =
    windowKey === "1d"
      ? {
          rowsBySymbol: new Map(),
          loadError: "",
        }
      : historyState;

  const spotRolling = normalized.spotRollingMaps[windowKey];
  const spotItems = buildSpotItems(normalized.spotSymbols, spotRolling);
  const { items: usdmItems, diagnostics } = buildUsdmItems(
    windowKey,
    normalized.usdmSymbols,
    normalized.usdm24hMap,
    effectiveHistoryState,
    true,
  );
  const items = [...spotItems, ...usdmItems];

  return {
    generatedAt: normalized.capturedAt,
    window: windowKey,
    windowLabel: WINDOW_CONFIG[windowKey].label,
    counts: {
      spot: spotItems.length,
      usdm: usdmItems.length,
      total: items.length,
    },
    diagnostics: {
      usdmHistoryFailures: diagnostics.historyFailures,
      usdmInsufficientHistory: diagnostics.insufficientHistory,
    },
    notes: buildNotes(windowKey, diagnostics, {
      source: normalized.source,
      historyLoadError: effectiveHistoryState.loadError,
    }),
    items,
  };
}

async function maybeRepairUsdmSnapshot(env, snapshot, windowKey) {
  if (!snapshot || snapshot.counts?.usdm > 0 || !hasD1(env)) {
    return snapshot;
  }

  try {
    const referenceTimeMs = parseTimestamp(snapshot.generatedAt) ?? Date.now();
    const historyState = await loadUsdmHistoryRows(env, WINDOW_CONFIG["7d"].days, referenceTimeMs);
    const fallbackItems = buildUsdmHistoryFallbackItems(windowKey, historyState);

    if (fallbackItems.length === 0) {
      return addSnapshotNote(snapshot, {
        kind: "warn",
        text: `U 本位永续 ${windowKey} 数据当前不可用；最近一次快照未能写入合约结果。`,
      });
    }

    const spotItems = Array.isArray(snapshot.items)
      ? snapshot.items.filter((item) => item?.segment !== "usdm")
      : [];

    return {
      ...snapshot,
      counts: {
        spot: spotItems.length,
        usdm: fallbackItems.length,
        total: spotItems.length + fallbackItems.length,
      },
      diagnostics: {
        ...(snapshot.diagnostics ?? {}),
        usdmRecoveredFromHistory: fallbackItems.length,
      },
      notes: addSnapshotNote(snapshot, {
        kind: "warn",
        text: `U 本位永续 ${windowKey} 当前由 D1 历史快照回填，说明最新一次 Binance Futures 采集失败。`,
      }).notes,
      items: [...spotItems, ...fallbackItems],
    };
  } catch (error) {
    return addSnapshotNote(snapshot, {
      kind: "warn",
      text: `U 本位永续 ${windowKey} 回填失败：${error?.message ? String(error.message) : "未知错误"}。`,
    });
  }
}

function buildUsdmHistoryFallbackItems(windowKey, historyState) {
  const items = [];
  const days = WINDOW_CONFIG[windowKey].days;

  for (const [symbol, rows] of historyState.rowsBySymbol) {
    const recentRows = days === 1 ? rows.slice(-1) : rows.slice(-days);
    const latestRow = recentRows.at(-1) ?? rows.at(-1);
    if (!latestRow) {
      continue;
    }

    const lastPrice = toNumber(latestRow.lastPrice);
    const referencePrice = toNumber(recentRows[0]?.openPrice);
    const windowHigh = getMaxNumber(recentRows.map((row) => toNumber(row.highPrice)));
    const windowLow = getMinNumber(recentRows.map((row) => toNumber(row.lowPrice)));
    const changePercent = percentChange(lastPrice, referencePrice);
    const rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);

    if (!Number.isFinite(lastPrice) || !Number.isFinite(changePercent)) {
      continue;
    }

    const uniqueDays = new Set(recentRows.map((row) => row.snapshotDay)).size;
    const hasEnoughHistory = recentRows.length >= days && uniqueDays >= days;

    items.push({
      market: "perpetual",
      segment: "usdm",
      segmentLabel: "U本位永续",
      symbol,
      displaySymbol: createDisplaySymbol(getBaseAssetFromSymbol(symbol), QUOTE_ASSET),
      baseAsset: getBaseAssetFromSymbol(symbol),
      quoteAsset: QUOTE_ASSET,
      lastPrice,
      referencePrice,
      changePercent,
      windowHigh,
      windowLow,
      rangePercent,
      isExact: false,
      dataStatus: hasEnoughHistory ? "history-fallback" : "history-short",
      dataIssue: hasEnoughHistory ? "使用D1回填" : `历史不足 ${windowKey}`,
    });
  }

  return items;
}

function addSnapshotNote(snapshot, note) {
  const notes = Array.isArray(snapshot?.notes) ? snapshot.notes : [];
  const exists = notes.some((item) => item?.kind === note.kind && item?.text === note.text);
  if (exists) {
    return snapshot;
  }

  return {
    ...snapshot,
    notes: [note, ...notes],
  };
}

function hasD1(env) {
  return Boolean(env?.DB);
}

async function ensureDbSchema(env) {
  if (!hasD1(env)) {
    return;
  }

  if (!schemaPromise) {
    schemaPromise = env.DB.batch([
      env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS snapshot_cache (
          window_key TEXT PRIMARY KEY,
          payload TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          expires_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `),
      env.DB.prepare(`
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
        )
      `),
      env.DB.prepare(`
        CREATE INDEX IF NOT EXISTS idx_usdm_daily_stats_symbol_day
        ON usdm_daily_stats (symbol, snapshot_day)
      `),
    ]).catch((error) => {
      schemaPromise = null;
      throw error;
    });
  }

  await schemaPromise;
}

async function readSnapshotCache(env, windowKey) {
  await ensureDbSchema(env);
  const row = await env.DB.prepare(
    `
      SELECT payload, expires_at
      FROM snapshot_cache
      WHERE window_key = ?
    `,
  )
    .bind(windowKey)
    .first();

  if (!row?.payload) {
    return null;
  }

  try {
    return {
      value: JSON.parse(row.payload),
      expiresAt: Number(row.expires_at) || 0,
    };
  } catch {
    return null;
  }
}

async function writeSnapshotCache(env, windowKey, payload, expiresAt) {
  await ensureDbSchema(env);
  const updatedAt = Date.now();

  await env.DB.prepare(
    `
      INSERT INTO snapshot_cache (
        window_key,
        payload,
        generated_at,
        expires_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(window_key) DO UPDATE SET
        payload = excluded.payload,
        generated_at = excluded.generated_at,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at
    `,
  )
    .bind(windowKey, JSON.stringify(payload), payload.generatedAt, expiresAt, updatedAt)
    .run();
}

async function upsertUsdmDailyStats(env, usdmSymbols, tickers, capturedAtMs) {
  await ensureDbSchema(env);
  const snapshotDay = getUtcDayKey(new Date(capturedAtMs));

  const countRow = await env.DB.prepare(
    `
      SELECT COUNT(*) AS rowCount
      FROM usdm_daily_stats
      WHERE snapshot_day = ?
    `,
  )
    .bind(snapshotDay)
    .first();

  const existingCount = toNumber(countRow?.rowCount) ?? 0;
  if (existingCount >= usdmSymbols.length && usdmSymbols.length > 0) {
    return;
  }

  const capturedAt = new Date(capturedAtMs).toISOString();
  const statements = [];

  for (const symbolMeta of usdmSymbols) {
    const ticker = tickers.get(symbolMeta.symbol);
    if (!ticker) {
      continue;
    }

    const openPrice = toNumber(ticker.openPrice);
    const highPrice = toNumber(ticker.highPrice);
    const lowPrice = toNumber(ticker.lowPrice);
    const lastPrice = toNumber(ticker.lastPrice ?? ticker.price);

    if (
      !Number.isFinite(openPrice) ||
      !Number.isFinite(highPrice) ||
      !Number.isFinite(lowPrice) ||
      !Number.isFinite(lastPrice)
    ) {
      continue;
    }

    statements.push(
      env.DB.prepare(
        `
          INSERT INTO usdm_daily_stats (
            snapshot_day,
            symbol,
            base_asset,
            quote_asset,
            open_price,
            high_price,
            low_price,
            last_price,
            captured_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(snapshot_day, symbol) DO UPDATE SET
            base_asset = excluded.base_asset,
            quote_asset = excluded.quote_asset,
            open_price = excluded.open_price,
            high_price = excluded.high_price,
            low_price = excluded.low_price,
            last_price = excluded.last_price,
            captured_at = excluded.captured_at
        `,
      ).bind(
        snapshotDay,
        symbolMeta.symbol,
        symbolMeta.baseAsset,
        symbolMeta.quoteAsset,
        openPrice,
        highPrice,
        lowPrice,
        lastPrice,
        capturedAt,
      ),
    );
  }

  for (const batch of chunk(statements, 50)) {
    if (batch.length > 0) {
      await env.DB.batch(batch);
    }
  }
}

async function loadUsdmHistoryRows(env, days, referenceTimeMs) {
  if (days <= 0) {
    return {
      rowsBySymbol: new Map(),
      loadError: "",
    };
  }

  await ensureDbSchema(env);
  const startDay = getUtcDayKey(new Date(referenceTimeMs - (days - 1) * DAY_MS));
  const result = await env.DB.prepare(
    `
      SELECT
        snapshot_day AS snapshotDay,
        symbol,
        base_asset AS baseAsset,
        quote_asset AS quoteAsset,
        open_price AS openPrice,
        high_price AS highPrice,
        low_price AS lowPrice,
        last_price AS lastPrice,
        captured_at AS capturedAt
      FROM usdm_daily_stats
      WHERE snapshot_day >= ?
      ORDER BY symbol ASC, snapshot_day ASC
    `,
  )
    .bind(startDay)
    .all();

  const rowsBySymbol = new Map();
  for (const row of result.results ?? []) {
    if (!rowsBySymbol.has(row.symbol)) {
      rowsBySymbol.set(row.symbol, []);
    }

    rowsBySymbol.get(row.symbol).push(row);
  }

  return {
    rowsBySymbol,
    loadError: "",
  };
}

function getUtcDayKey(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function chunk(list, size) {
  const result = [];
  for (let index = 0; index < list.length; index += size) {
    result.push(list.slice(index, index + size));
  }
  return result;
}

function toNumber(value) {
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function percentChange(lastPrice, referencePrice) {
  if (!Number.isFinite(lastPrice) || !Number.isFinite(referencePrice) || referencePrice === 0) {
    return null;
  }

  return ((lastPrice - referencePrice) / referencePrice) * 100;
}

function priceRangePercent(highPrice, lowPrice, basePrice) {
  if (
    !Number.isFinite(highPrice) ||
    !Number.isFinite(lowPrice) ||
    !Number.isFinite(basePrice) ||
    basePrice === 0
  ) {
    return null;
  }

  return ((highPrice - lowPrice) / basePrice) * 100;
}

function formatListPreview(items, limit = 6) {
  if (!Array.isArray(items) || items.length === 0) {
    return "";
  }

  const preview = items.slice(0, limit).join(", ");
  const remaining = items.length - limit;
  return remaining > 0 ? `${preview} 等 ${items.length} 个` : preview;
}

function createDisplaySymbol(baseAsset, quoteAsset) {
  return baseAsset && quoteAsset ? `${baseAsset}/${quoteAsset}` : "";
}

function getBaseAssetFromSymbol(symbol) {
  if (typeof symbol !== "string" || !symbol.endsWith(QUOTE_ASSET)) {
    return symbol ?? "";
  }

  return symbol.slice(0, -QUOTE_ASSET.length);
}

function isUsdtTradingSymbol(symbol) {
  return (
    typeof symbol === "string" &&
    symbol.endsWith(QUOTE_ASSET) &&
    !symbol.includes("_") &&
    symbol.length > QUOTE_ASSET.length
  );
}

function createSymbolMeta({ market, segment, segmentLabel, symbol }) {
  return {
    market,
    segment,
    segmentLabel,
    symbol,
    baseAsset: getBaseAssetFromSymbol(symbol),
    quoteAsset: QUOTE_ASSET,
  };
}

function buildSpotItems(symbols, rollingStats) {
  const items = [];

  for (const symbol of symbols) {
    const stats = rollingStats.get(symbol.symbol);
    if (!stats) {
      continue;
    }

    const lastPrice = toNumber(stats.lastPrice);
    const referencePrice = toNumber(stats.openPrice);
    const windowHigh = toNumber(stats.highPrice);
    const windowLow = toNumber(stats.lowPrice);
    const changePercent = toNumber(stats.priceChangePercent);
    const rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);

    if (!Number.isFinite(lastPrice) || !Number.isFinite(changePercent)) {
      continue;
    }

    items.push({
      market: "spot",
      segment: "spot",
      segmentLabel: "现货",
      symbol: symbol.symbol,
      displaySymbol: createDisplaySymbol(symbol.baseAsset, symbol.quoteAsset),
      baseAsset: symbol.baseAsset,
      quoteAsset: symbol.quoteAsset,
      lastPrice,
      referencePrice,
      changePercent,
      windowHigh,
      windowLow,
      rangePercent,
      isExact: true,
      dataStatus: "ready",
      dataIssue: "",
    });
  }

  return items;
}

function buildUsdmItems(windowKey, symbols, tickers, historyState, hasDatabase) {
  const items = [];
  const diagnostics = {
    historyFailures: [],
    insufficientHistory: [],
  };

  for (const symbol of symbols) {
    const ticker = tickers.get(symbol.symbol);
    if (!ticker) {
      continue;
    }

    const lastPrice = toNumber(ticker.lastPrice ?? ticker.price);
    if (!Number.isFinite(lastPrice)) {
      continue;
    }

    let referencePrice = null;
    let changePercent = null;
    let windowHigh = null;
    let windowLow = null;
    let rangePercent = null;
    let isExact = false;
    let dataStatus = "ready";
    let dataIssue = "";

    if (windowKey === "1d") {
      referencePrice = toNumber(ticker.openPrice);
      windowHigh = toNumber(ticker.highPrice);
      windowLow = toNumber(ticker.lowPrice);
      changePercent =
        toNumber(ticker.priceChangePercent) ?? percentChange(lastPrice, referencePrice);
      rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);
      isExact = true;
    } else if (historyState.loadError) {
      dataStatus = "history-failed";
      dataIssue = "D1历史失败";
      diagnostics.historyFailures.push(symbol.symbol);
    } else if (!hasDatabase) {
      dataStatus = "history-short";
      dataIssue = "未配置D1";
      diagnostics.insufficientHistory.push(symbol.symbol);
    } else {
      const rows = historyState.rowsBySymbol.get(symbol.symbol) ?? [];
      const recentRows = rows.slice(-WINDOW_CONFIG[windowKey].days);
      const uniqueDays = new Set(recentRows.map((row) => row.snapshotDay)).size;

      referencePrice = toNumber(recentRows[0]?.openPrice);
      windowHigh = getMaxNumber(recentRows.map((row) => toNumber(row.highPrice)));
      windowLow = getMinNumber(recentRows.map((row) => toNumber(row.lowPrice)));
      changePercent = percentChange(lastPrice, referencePrice);
      rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);
      isExact = false;

      if (recentRows.length < WINDOW_CONFIG[windowKey].days || uniqueDays < WINDOW_CONFIG[windowKey].days) {
        dataStatus = "history-short";
        dataIssue = `历史不足 ${windowKey}`;
        diagnostics.insufficientHistory.push(symbol.symbol);
      }
    }

    if (dataStatus === "ready" && !Number.isFinite(changePercent)) {
      continue;
    }

    items.push({
      market: "perpetual",
      segment: "usdm",
      segmentLabel: "U本位永续",
      symbol: symbol.symbol,
      displaySymbol: createDisplaySymbol(symbol.baseAsset, symbol.quoteAsset),
      baseAsset: symbol.baseAsset,
      quoteAsset: symbol.quoteAsset,
      lastPrice,
      referencePrice,
      changePercent,
      windowHigh,
      windowLow,
      rangePercent,
      isExact,
      dataStatus,
      dataIssue,
    });
  }

  return {
    items,
    diagnostics,
  };
}

function getMaxNumber(values) {
  let result = -Infinity;
  for (const value of values) {
    if (Number.isFinite(value)) {
      result = Math.max(result, value);
    }
  }
  return Number.isFinite(result) ? result : null;
}

function getMinNumber(values) {
  let result = Infinity;
  for (const value of values) {
    if (Number.isFinite(value)) {
      result = Math.min(result, value);
    }
  }
  return Number.isFinite(result) ? result : null;
}

function buildNotes(windowKey, diagnostics, context) {
  const notes = [
    {
      kind: "info",
      text: "当前页面只读取 D1 已入库快照；不会在用户请求时直连 Binance。",
    },
    {
      kind: "info",
      text: `快照由外部定时任务写入 Worker，再落到 D1。当前来源：${context.source}。`,
    },
    {
      kind: "info",
      text: "当前只统计 USDT 交易对；只保留现货和 U 本位永续。",
    },
    {
      kind: "exact",
      text: `现货 ${windowKey} 排行使用 Binance Spot rolling window 接口结果，属于精确窗口值。`,
    },
    {
      kind: windowKey === "1d" ? "exact" : "approx",
      text:
        windowKey === "1d"
          ? "U 本位永续 1d 排行使用 Binance 24h ticker。"
          : "U 本位永续 3d/7d 使用 D1 中累积的日快照近似计算；首次启用后需要累计到对应天数。",
    },
  ];

  if (windowKey !== "1d" && context.historyLoadError) {
    notes.push({
      kind: "warn",
      text: `D1 历史读取失败：${context.historyLoadError}。`,
    });
  }

  if (windowKey !== "1d" && diagnostics.historyFailures.length > 0) {
    notes.push({
      kind: "warn",
      text: `U 本位永续 ${windowKey} 历史读取失败 ${diagnostics.historyFailures.length} 个标的。示例：${formatListPreview(diagnostics.historyFailures)}。`,
    });
  }

  if (windowKey !== "1d" && diagnostics.insufficientHistory.length > 0) {
    notes.push({
      kind: "warn",
      text: `U 本位永续 ${windowKey} 还有 ${diagnostics.insufficientHistory.length} 个标的历史不足。示例：${formatListPreview(diagnostics.insufficientHistory)}。`,
    });
  }

  return notes;
}
