const QUOTE_ASSET = "USDT";
const STORAGE_PREFIX = "binance-pages-static-v2";

const windowOptions = [
  { key: "1d", label: "1天" },
  { key: "3d", label: "3天" },
  { key: "7d", label: "7天" },
];

const WINDOW_CONFIG = {
  "1d": {
    label: "1天",
    ttlMs: 60_000,
    durationMs: 86_400_000,
    usdmHistory: null,
  },
  "3d": {
    label: "3天",
    ttlMs: 5 * 60_000,
    durationMs: 3 * 86_400_000,
    usdmHistory: { interval: "1d", limit: 8 },
  },
  "7d": {
    label: "7天",
    ttlMs: 5 * 60_000,
    durationMs: 7 * 86_400_000,
    usdmHistory: { interval: "1d", limit: 8 },
  },
};

const spotConfig = {
  title: "现货",
  baseUrls: [
    "https://data-api.binance.vision",
    "https://api.binance.com",
  ],
  exchangePath: "/api/v3/exchangeInfo",
  rollingPath: "/api/v3/ticker",
  rollingBatchSize: 20,
  rollingConcurrency: 3,
};

const usdmConfig = {
  title: "U本位永续",
  baseUrls: [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
  ],
  exchangePath: "/fapi/v1/exchangeInfo",
  ticker24hPath: "/fapi/v1/ticker/24hr",
  pricePath: "/fapi/v1/ticker/price",
  klinePath: "/fapi/v1/klines",
  historyConcurrency: 3,
  historyRetryRounds: 3,
};

const runtimeCache = new Map();

const state = {
  selectedWindow: "1d",
  selectedSegment: "all",
  leaderboardLimit: 20,
  searchText: "",
  payload: null,
  loading: false,
  loadId: 0,
  refreshTimer: null,
};

const elements = {
  windowTabs: document.querySelector("#windowTabs"),
  segmentFilter: document.querySelector("#segmentFilter"),
  leaderboardLimit: document.querySelector("#leaderboardLimit"),
  searchInput: document.querySelector("#searchInput"),
  refreshButton: document.querySelector("#refreshButton"),
  statusText: document.querySelector("#statusText"),
  notesList: document.querySelector("#notesList"),
  generatedAt: document.querySelector("#generatedAt"),
  countTotal: document.querySelector("#countTotal"),
  countSpot: document.querySelector("#countSpot"),
  countUsdm: document.querySelector("#countUsdm"),
  heroWindowLabel: document.querySelector("#heroWindowLabel"),
  gainerWindowHead: document.querySelector("#gainerWindowHead"),
  loserWindowHead: document.querySelector("#loserWindowHead"),
  activityChangeHead: document.querySelector("#activityChangeHead"),
  activityRangeHead: document.querySelector("#activityRangeHead"),
  gainersBody: document.querySelector("#gainersBody"),
  losersBody: document.querySelector("#losersBody"),
  activityBody: document.querySelector("#activityBody"),
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunk(list, size) {
  const result = [];
  for (let index = 0; index < list.length; index += size) {
    result.push(list.slice(index, index + size));
  }
  return result;
}

function formatListPreview(items, limit = 6) {
  if (!Array.isArray(items) || items.length === 0) {
    return "";
  }

  const preview = items.slice(0, limit).join(", ");
  const remaining = items.length - limit;
  return remaining > 0 ? `${preview} 等 ${items.length} 个` : preview;
}

async function promisePool(items, concurrency, worker) {
  const results = new Array(items.length);
  let cursor = 0;

  async function runWorker() {
    while (cursor < items.length) {
      const currentIndex = cursor;
      cursor += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, items.length || 1));
  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  return results;
}

function readStoredEntry(key) {
  try {
    const raw = localStorage.getItem(`${STORAGE_PREFIX}:${key}`);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return parsed;
  } catch {
    return null;
  }
}

function writeStoredEntry(key, entry) {
  try {
    localStorage.setItem(`${STORAGE_PREFIX}:${key}`, JSON.stringify(entry));
  } catch {
    // Ignore storage quota failures.
  }
}

async function withCache(key, ttlMs, loader, options = {}) {
  const force = options.force === true;
  const now = Date.now();
  const cached = runtimeCache.get(key);

  if (!force && cached?.value && cached.expiresAt > now) {
    return cached.value;
  }

  if (!force) {
    const stored = readStoredEntry(key);
    if (stored?.value && stored.expiresAt > now) {
      runtimeCache.set(key, stored);
      return stored.value;
    }
  }

  if (!force && cached?.promise) {
    return cached.promise;
  }

  const promise = (async () => {
    const value = await loader();
    const entry = { value, expiresAt: Date.now() + ttlMs };
    runtimeCache.set(key, entry);
    writeStoredEntry(key, entry);
    return value;
  })();

  runtimeCache.set(key, {
    value: cached?.value,
    expiresAt: cached?.expiresAt ?? 0,
    promise,
  });

  try {
    return await promise;
  } catch (error) {
    if (cached) {
      runtimeCache.set(key, cached);
    } else {
      runtimeCache.delete(key);
    }
    throw error;
  }
}

async function fetchJson(url, options = {}) {
  const {
    timeoutMs = 20_000,
    retries = 1,
    label = url.toString(),
    retryDelayMs = 800,
  } = options;

  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
        },
      });

      if (!response.ok) {
        const body = (await response.text()).slice(0, 240);
        throw new Error(`${label} failed with ${response.status}: ${body}`);
      }

      const rawText = await response.text();
      try {
        return JSON.parse(rawText);
      } catch {
        throw new Error(`${label} returned invalid JSON: ${rawText.slice(0, 240) || "<empty>"}`);
      }
    } catch (error) {
      lastError = error;
      const shouldRetry =
        attempt < retries &&
        (error.name === "AbortError" || error.message.includes("fetch"));

      if (shouldRetry) {
        await sleep(retryDelayMs * (attempt + 1));
      } else {
        break;
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError ?? new Error(`${label} failed`);
}

async function fetchJsonFromCandidates(baseUrls, pathname, configureUrl, options = {}) {
  let lastError = null;

  for (const baseUrl of baseUrls) {
    const url = new URL(pathname, baseUrl);
    if (typeof configureUrl === "function") {
      configureUrl(url);
    }

    try {
      return await fetchJson(url, {
        ...options,
        label: `${options.label ?? pathname} via ${baseUrl}`,
      });
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ?? new Error(`${pathname} failed on all base URLs`);
}

function toErrorMessage(error) {
  if (!error) {
    return "Unknown error";
  }

  const message = error?.message ? String(error.message) : String(error);
  return message.replace(/\s+/g, " ").trim().slice(0, 160);
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

function getReferenceBar(klines, targetTime) {
  if (!Array.isArray(klines) || klines.length === 0) {
    return null;
  }

  let candidate = null;
  for (const bar of klines) {
    const closeTime = Number(bar?.[6]);
    if (!Number.isFinite(closeTime)) {
      continue;
    }

    if (closeTime <= targetTime) {
      candidate = bar;
      continue;
    }

    break;
  }

  return candidate;
}

function getWindowHighLow(klines, targetTime, endTime) {
  if (!Array.isArray(klines) || klines.length === 0) {
    return null;
  }

  let highest = -Infinity;
  let lowest = Infinity;
  let seen = false;

  for (const bar of klines) {
    const openTime = Number(bar?.[0]);
    const closeTime = Number(bar?.[6]);
    const highPrice = toNumber(bar?.[2]);
    const lowPrice = toNumber(bar?.[3]);

    if (
      !Number.isFinite(openTime) ||
      !Number.isFinite(closeTime) ||
      !Number.isFinite(highPrice) ||
      !Number.isFinite(lowPrice)
    ) {
      continue;
    }

    if (closeTime > targetTime && openTime <= endTime) {
      highest = Math.max(highest, highPrice);
      lowest = Math.min(lowest, lowPrice);
      seen = true;
    }
  }

  if (!seen) {
    return null;
  }

  return {
    highPrice: highest,
    lowPrice: lowest,
  };
}

function createDisplaySymbol(baseAsset, quoteAsset) {
  return baseAsset && quoteAsset ? `${baseAsset}/${quoteAsset}` : "";
}

function isSpotSymbolEligible(symbol) {
  const permissions = Array.isArray(symbol.permissions) ? symbol.permissions : [];
  const permissionSets = Array.isArray(symbol.permissionSets)
    ? symbol.permissionSets.flat().filter(Boolean)
    : [];
  const hasSpotPermission =
    Boolean(symbol.isSpotTradingAllowed) ||
    permissions.includes("SPOT") ||
    permissionSets.includes("SPOT");

  return symbol.status === "TRADING" && hasSpotPermission && symbol.quoteAsset === QUOTE_ASSET;
}

function isUsdmSymbolEligible(symbol) {
  return (
    symbol.status === "TRADING" &&
    symbol.contractType === "PERPETUAL" &&
    symbol.quoteAsset === QUOTE_ASSET
  );
}

async function getSpotSymbols(options = {}) {
  return withCache("symbols:spot", 60 * 60 * 1000, async () => {
    const payload = await fetchJsonFromCandidates(spotConfig.baseUrls, spotConfig.exchangePath, null, {
      label: "Spot exchange info",
      retries: 2,
    });

    return (payload.symbols ?? [])
      .filter(isSpotSymbolEligible)
      .map((symbol) => ({
        market: "spot",
        segment: "spot",
        segmentLabel: "现货",
        symbol: symbol.symbol,
        baseAsset: symbol.baseAsset,
        quoteAsset: symbol.quoteAsset,
      }));
  }, options);
}

async function getUsdmSymbols(options = {}) {
  return withCache("symbols:usdm", 60 * 60 * 1000, async () => {
    const payload = await fetchJsonFromCandidates(usdmConfig.baseUrls, usdmConfig.exchangePath, null, {
      label: "USD-M exchange info",
      retries: 2,
    });

    return (payload.symbols ?? [])
      .filter(isUsdmSymbolEligible)
      .map((symbol) => ({
        market: "perpetual",
        segment: "usdm",
        segmentLabel: "U本位永续",
        symbol: symbol.symbol,
        baseAsset: symbol.baseAsset,
        quoteAsset: symbol.quoteAsset,
      }));
  }, options);
}

async function getSpotRollingWindow(window, symbols, options = {}) {
  return withCache(`spot:rolling:${window}`, WINDOW_CONFIG[window].ttlMs, async () => {
    const batches = chunk(symbols.map((item) => item.symbol), spotConfig.rollingBatchSize);
    const pages = await promisePool(batches, spotConfig.rollingConcurrency, async (batch) => {
      return fetchJsonFromCandidates(
        spotConfig.baseUrls,
        spotConfig.rollingPath,
        (url) => {
          url.searchParams.set("windowSize", window);
          url.searchParams.set("type", "FULL");
          url.searchParams.set("symbols", JSON.stringify(batch));
        },
        {
          label: `Spot ${window} rolling window`,
          retries: 2,
        },
      );
    });

    const result = new Map();
    for (const page of pages) {
      for (const item of page) {
        result.set(item.symbol, item);
      }
    }
    return result;
  }, options);
}

async function getUsdm24hAllTickers(options = {}) {
  return withCache("usdm:24h", 60_000, async () => {
    const payload = await fetchJsonFromCandidates(usdmConfig.baseUrls, usdmConfig.ticker24hPath, null, {
      label: "USD-M 24h tickers",
      retries: 2,
    });

    const result = new Map();
    for (const item of payload ?? []) {
      if (item?.symbol) {
        result.set(item.symbol, item);
      }
    }
    return result;
  }, options);
}

async function getUsdmPrices(options = {}) {
  return withCache("usdm:price", 60_000, async () => {
    const payload = await fetchJsonFromCandidates(usdmConfig.baseUrls, usdmConfig.pricePath, null, {
      label: "USD-M latest prices",
      retries: 2,
    });

    const result = new Map();
    for (const item of payload ?? []) {
      if (item?.symbol) {
        result.set(item.symbol, item);
      }
    }
    return result;
  }, options);
}

async function getUsdmHistory(window, symbols, options = {}) {
  const historyConfig = WINDOW_CONFIG[window].usdmHistory;
  if (!historyConfig) {
    return {
      data: new Map(),
      failures: new Map(),
    };
  }

  return withCache(`usdm:history:${window}`, WINDOW_CONFIG[window].ttlMs, async () => {
    const entries = await promisePool(symbols, usdmConfig.historyConcurrency, async (symbolMeta) => {
      let lastError = null;

      for (let attempt = 0; attempt < usdmConfig.historyRetryRounds; attempt += 1) {
        try {
          const payload = await fetchJsonFromCandidates(
            usdmConfig.baseUrls,
            usdmConfig.klinePath,
            (url) => {
              url.searchParams.set("symbol", symbolMeta.symbol);
              url.searchParams.set("interval", historyConfig.interval);
              url.searchParams.set("limit", String(historyConfig.limit));
            },
            {
              label: `USD-M ${window} klines ${symbolMeta.symbol} attempt ${attempt + 1}`,
              retries: 2,
              timeoutMs: 20_000,
              retryDelayMs: 1_000 + attempt * 500,
            },
          );

          return {
            symbol: symbolMeta.symbol,
            payload,
            error: null,
          };
        } catch (error) {
          lastError = error;

          if (attempt < usdmConfig.historyRetryRounds - 1) {
            await sleep(700 * (attempt + 1));
          }
        }
      }

      return {
        symbol: symbolMeta.symbol,
        payload: null,
        error: toErrorMessage(lastError),
      };
    });

    const data = new Map();
    const failures = new Map();

    for (const entry of entries) {
      if (Array.isArray(entry?.payload)) {
        data.set(entry.symbol, entry.payload);
      } else if (entry?.symbol) {
        failures.set(entry.symbol, entry?.error ?? "Failed to load klines");
      }
    }

    return { data, failures };
  }, options);
}

function buildSpotItems(window, symbols, rollingStats) {
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
    });
  }

  return items;
}

function buildUsdmItems(window, symbols, tickers, histories) {
  const items = [];
  const now = Date.now();
  const targetTime = now - WINDOW_CONFIG[window].durationMs;
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

    if (window === "1d") {
      referencePrice = toNumber(ticker.openPrice);
      windowHigh = toNumber(ticker.highPrice);
      windowLow = toNumber(ticker.lowPrice);
      changePercent =
        toNumber(ticker.priceChangePercent) ?? percentChange(lastPrice, referencePrice);
      rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);
      isExact = true;
    } else {
      const klines = histories.data.get(symbol.symbol);
      const historyError = histories.failures.get(symbol.symbol);
      const referenceBar = getReferenceBar(klines, targetTime);
      const highLow = getWindowHighLow(klines, targetTime, now);

      referencePrice = toNumber(referenceBar?.[4]);
      changePercent = percentChange(lastPrice, referencePrice);
      windowHigh = highLow?.highPrice ?? null;
      windowLow = highLow?.lowPrice ?? null;
      rangePercent = priceRangePercent(windowHigh, windowLow, referencePrice);
      isExact = false;

      if (historyError) {
        dataStatus = "history-failed";
        dataIssue = "K线加载失败";
        diagnostics.historyFailures.push(symbol.symbol);
      } else if (!Number.isFinite(referencePrice) || !Number.isFinite(rangePercent)) {
        dataStatus = "history-short";
        dataIssue = `历史不足 ${window}`;
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

function buildNotes(window, diagnostics = {}) {
  const notes = [
    {
      kind: "info",
      text: "纯静态前端版本由浏览器直接请求 Binance；当前浏览器网络必须能访问 Binance。",
    },
    {
      kind: "info",
      text: "当前只统计 USDT 交易对；只保留现货和 U 本位永续。",
    },
    {
      kind: "info",
      text: "异动榜单按 (周期最高价 - 周期最低价) / 窗口起点价 排序；1d 使用 24h openPrice，3d/7d 使用窗口起点参考价。",
    },
    {
      kind: "exact",
      text: `现货 ${window} 排行使用 Binance Spot rolling window 接口，属于精确窗口值。`,
    },
    {
      kind: window === "1d" ? "exact" : "approx",
      text:
        window === "1d"
          ? "U 本位永续 1d 排行使用 Binance 24h ticker。"
          : "U 本位永续 3d/7d 使用 1d K 线近似计算，因此和滚动窗口可能有轻微偏差。",
    },
  ];

  if (window !== "1d" && diagnostics.historyFailures?.length) {
    notes.push({
      kind: "warn",
      text: `U 本位永续 ${window} K 线有 ${diagnostics.historyFailures.length} 个标的加载失败；这些标的已保留在数据集里并标记为“缺失”。示例：${formatListPreview(diagnostics.historyFailures)}。`,
    });
  }

  if (window !== "1d" && diagnostics.insufficientHistory?.length) {
    notes.push({
      kind: "warn",
      text: `U 本位永续 ${window} 有 ${diagnostics.insufficientHistory.length} 个标的历史不足；这些标的已保留在数据集里并标记为“缺失”。示例：${formatListPreview(diagnostics.insufficientHistory)}。`,
    });
  }

  return notes;
}

async function buildSnapshot(window, options = {}, setStage = () => {}) {
  setStage("加载交易对列表...");
  const [spotSymbols, usdmSymbols] = await Promise.all([
    getSpotSymbols(options),
    getUsdmSymbols(options),
  ]);

  setStage(`加载 ${window} 现货榜单...`);
  const spotRolling = await getSpotRollingWindow(window, spotSymbols, options);

  let usdmTickers = new Map();
  let usdmHistory = {
    data: new Map(),
    failures: new Map(),
  };

  if (window === "1d") {
    setStage("加载 1d 永续榜单...");
    usdmTickers = await getUsdm24hAllTickers(options);
  } else {
    setStage(`加载 ${window} 永续最新价...`);
    usdmTickers = await getUsdmPrices(options);

    setStage(`加载 ${window} 永续 K 线，首次会慢一些...`);
    usdmHistory = await getUsdmHistory(window, usdmSymbols, options);
  }

  const spotItems = buildSpotItems(window, spotSymbols, spotRolling);
  const { items: usdmItems, diagnostics: usdmDiagnostics } = buildUsdmItems(
    window,
    usdmSymbols,
    usdmTickers,
    usdmHistory,
  );
  const items = [...spotItems, ...usdmItems];

  return {
    generatedAt: new Date().toISOString(),
    window,
    windowLabel: WINDOW_CONFIG[window].label,
    counts: {
      spot: spotItems.length,
      usdm: usdmItems.length,
      total: items.length,
    },
    diagnostics: {
      usdmHistoryFailures: usdmDiagnostics.historyFailures,
      usdmInsufficientHistory: usdmDiagnostics.insufficientHistory,
    },
    notes: buildNotes(window, usdmDiagnostics),
    items,
  };
}

async function getSnapshot(window, options = {}, setStage = () => {}) {
  return withCache(
    `snapshot:${window}`,
    WINDOW_CONFIG[window].ttlMs,
    () => buildSnapshot(window, options, setStage),
    options,
  );
}

function updateWindowLabels() {
  const label = WINDOW_CONFIG[state.selectedWindow]?.label ?? state.selectedWindow;
  elements.heroWindowLabel.textContent = label;
  elements.gainerWindowHead.textContent = `${label}涨跌幅`;
  elements.loserWindowHead.textContent = `${label}涨跌幅`;
  elements.activityChangeHead.textContent = `${label}涨跌幅`;
  elements.activityRangeHead.textContent = `${label}异动幅度`;
}

function renderWindowTabs() {
  elements.windowTabs.innerHTML = windowOptions
    .map(
      (option) => `
        <button
          class="tab-button ${option.key === state.selectedWindow ? "is-active" : ""}"
          type="button"
          data-window="${option.key}"
        >
          ${option.label}
        </button>
      `,
    )
    .join("");

  elements.windowTabs.querySelectorAll("[data-window]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextWindow = button.getAttribute("data-window");
      if (nextWindow === state.selectedWindow) {
        return;
      }

      state.selectedWindow = nextWindow;
      renderWindowTabs();
      updateWindowLabels();
      loadSnapshot();
    });
  });
}

function formatPercent(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatUnsignedPercent(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  return `${value.toFixed(2)}%`;
}

function formatPrice(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  if (value >= 1000) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }

  if (value >= 1) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    });
  }

  if (value >= 0.01) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 4,
      maximumFractionDigits: 8,
    });
  }

  return value.toLocaleString("en-US", {
    minimumFractionDigits: 6,
    maximumFractionDigits: 10,
  });
}

function formatTime(value) {
  if (!value) {
    return "-";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function getValueClass(value) {
  if (!Number.isFinite(value) || value === 0) {
    return "value-neutral";
  }

  return value > 0 ? "value-positive" : "value-negative";
}

function getFilteredItems() {
  const items = Array.isArray(state.payload?.items) ? state.payload.items : [];

  return items.filter((item) => {
    if (state.selectedSegment !== "all" && item.segment !== state.selectedSegment) {
      return false;
    }

    if (!state.searchText) {
      return true;
    }

    const keyword = state.searchText.toLowerCase();
    return (
      item.symbol.toLowerCase().includes(keyword) ||
      item.displaySymbol.toLowerCase().includes(keyword) ||
      item.segmentLabel.toLowerCase().includes(keyword)
    );
  });
}

function sortItems(items, mode) {
  const copy = [...items];

  copy.sort((left, right) => {
    if (mode === "asc") {
      return (left.changePercent ?? Infinity) - (right.changePercent ?? Infinity);
    }

    return (right.changePercent ?? -Infinity) - (left.changePercent ?? -Infinity);
  });

  return copy;
}

function sortByActivity(items) {
  const copy = [...items];

  copy.sort((left, right) => {
    const rangeDelta = (right.rangePercent ?? -Infinity) - (left.rangePercent ?? -Infinity);
    if (rangeDelta !== 0) {
      return rangeDelta;
    }

    return Math.abs(right.changePercent ?? 0) - Math.abs(left.changePercent ?? 0);
  });

  return copy;
}

function createSymbolCell(item) {
  return `
    <div class="symbol-cell">
      <span class="symbol-main">${item.displaySymbol || item.symbol}</span>
      <span class="symbol-sub">${item.symbol}</span>
      ${item.dataIssue ? `<span class="symbol-issue">${item.dataIssue}</span>` : ""}
    </div>
  `;
}

function getAccuracyMeta(item) {
  if (item.dataStatus === "history-failed") {
    return {
      className: "is-missing",
      label: "缺失",
    };
  }

  if (item.dataStatus === "history-short") {
    return {
      className: "is-missing",
      label: "历史不足",
    };
  }

  return {
    className: item.isExact ? "is-exact" : "is-approx",
    label: item.isExact ? "精确" : "近似",
  };
}

function createBoardRow(item, rank) {
  return `
    <tr>
      <td>${rank}</td>
      <td>${createSymbolCell(item)}</td>
      <td><span class="segment-badge" data-segment="${item.segment}">${item.segmentLabel}</span></td>
      <td>${formatPrice(item.lastPrice)}</td>
      <td class="${getValueClass(item.changePercent)}">${formatPercent(item.changePercent)}</td>
    </tr>
  `;
}

function createActivityRow(item, rank) {
  const accuracy = getAccuracyMeta(item);
  return `
    <tr>
      <td>${rank}</td>
      <td>${createSymbolCell(item)}</td>
      <td><span class="segment-badge" data-segment="${item.segment}">${item.segmentLabel}</span></td>
      <td>${formatPrice(item.lastPrice)}</td>
      <td class="${getValueClass(item.changePercent)}">${formatPercent(item.changePercent)}</td>
      <td>${formatPrice(item.windowHigh)}</td>
      <td>${formatPrice(item.windowLow)}</td>
      <td class="${getValueClass(item.rangePercent)}">${formatUnsignedPercent(item.rangePercent)}</td>
      <td>
        <span class="accuracy-badge ${accuracy.className}">
          ${accuracy.label}
        </span>
      </td>
    </tr>
  `;
}

function renderEmptyState(target, message, colspan) {
  target.innerHTML = `<tr><td class="empty-state" colspan="${colspan}">${message}</td></tr>`;
}

function renderNotes() {
  const notes = Array.isArray(state.payload?.notes) ? state.payload.notes : [];
  elements.notesList.innerHTML = notes
    .map(
      (note) => `
        <div class="note-row">
          <span class="note-tag is-${note.kind}">${note.kind}</span>
          <p>${note.text}</p>
        </div>
      `,
    )
    .join("");
}

function renderMeta() {
  elements.generatedAt.textContent = state.payload?.generatedAt
    ? formatTime(state.payload.generatedAt)
    : "-";

  elements.countTotal.textContent = state.payload?.counts?.total ?? "-";
  elements.countSpot.textContent = state.payload?.counts?.spot ?? "-";
  elements.countUsdm.textContent = state.payload?.counts?.usdm ?? "-";
}

function renderTables() {
  const filtered = getFilteredItems();
  const gainers = sortItems(filtered, "desc").slice(0, state.leaderboardLimit);
  const losers = sortItems(filtered, "asc").slice(0, state.leaderboardLimit);
  const activityItems = sortByActivity(filtered).slice(0, state.leaderboardLimit);

  if (gainers.length === 0) {
    renderEmptyState(elements.gainersBody, "当前筛选条件下没有数据。", 5);
  } else {
    elements.gainersBody.innerHTML = gainers.map((item, index) => createBoardRow(item, index + 1)).join("");
  }

  if (losers.length === 0) {
    renderEmptyState(elements.losersBody, "当前筛选条件下没有数据。", 5);
  } else {
    elements.losersBody.innerHTML = losers.map((item, index) => createBoardRow(item, index + 1)).join("");
  }

  if (activityItems.length === 0) {
    renderEmptyState(elements.activityBody, "当前筛选条件下没有数据。", 9);
  } else {
    elements.activityBody.innerHTML = activityItems
      .map((item, index) => createActivityRow(item, index + 1))
      .join("");
  }
}

function renderAll() {
  renderMeta();
  renderNotes();
  renderTables();
}

function setStatus(message) {
  elements.statusText.textContent = message;
}

async function loadSnapshot(options = {}) {
  const force = options.force === true;
  const currentLoadId = state.loadId + 1;
  state.loadId = currentLoadId;
  state.loading = true;

  try {
    const payload = await getSnapshot(
      state.selectedWindow,
      { force },
      (message) => {
        if (state.loadId === currentLoadId) {
          setStatus(message);
        }
      },
    );

    if (state.loadId !== currentLoadId) {
      return;
    }

    state.payload = payload;
    renderAll();
    const missingCount =
      (payload.diagnostics?.usdmHistoryFailures?.length ?? 0) +
      (payload.diagnostics?.usdmInsufficientHistory?.length ?? 0);
    const missingSuffix =
      payload.window !== "1d" && missingCount > 0
        ? ` 其中 ${missingCount} 个永续标的未完成 ${payload.windowLabel} 计算，已标记为缺失。`
        : "";
    setStatus(`已载入 ${payload.windowLabel} 榜单，共 ${payload.counts.total} 个标的。${missingSuffix}`);
  } catch (error) {
    if (state.loadId !== currentLoadId) {
      return;
    }

    if (state.payload) {
      setStatus("刷新失败，当前展示缓存数据。请确认浏览器网络能直连 Binance。");
      return;
    }

    setStatus(`加载失败：请确认当前浏览器网络能直连 Binance。${error.message ? ` (${error.message})` : ""}`);
    renderNotes();
    renderEmptyState(elements.gainersBody, "未能获取数据。", 5);
    renderEmptyState(elements.losersBody, "未能获取数据。", 5);
    renderEmptyState(elements.activityBody, "未能获取数据。", 9);
  } finally {
    if (state.loadId === currentLoadId) {
      state.loading = false;
    }
  }
}

function scheduleRefresh() {
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
  }

  state.refreshTimer = setInterval(() => {
    if (!document.hidden) {
      loadSnapshot();
    }
  }, 60_000);
}

function bindControls() {
  elements.segmentFilter.addEventListener("change", (event) => {
    state.selectedSegment = event.target.value;
    renderTables();
  });

  elements.leaderboardLimit.addEventListener("change", (event) => {
    state.leaderboardLimit = Number(event.target.value);
    renderTables();
  });

  elements.searchInput.addEventListener("input", (event) => {
    state.searchText = event.target.value.trim();
    renderTables();
  });

  elements.refreshButton.addEventListener("click", () => {
    loadSnapshot({ force: true });
  });
}

function bootstrap() {
  renderWindowTabs();
  updateWindowLabels();
  bindControls();
  scheduleRefresh();
  loadSnapshot();
}

bootstrap();
