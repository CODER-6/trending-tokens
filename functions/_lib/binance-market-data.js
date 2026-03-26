const QUOTE_ASSET = "USDT";
const SPOT_BASE_URLS = ["https://data-api.binance.vision", "https://api.binance.com"];
const USDM_BASE_URLS = [
  "https://fapi.binance.com",
  "https://fapi1.binance.com",
  "https://fapi2.binance.com",
  "https://fapi3.binance.com",
];
const SPOT_ROLLING_BATCH_SIZE = 50;

export async function collectMarketSnapshot(options = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const log = typeof options.log === "function" ? options.log : () => {};
  const source =
    typeof options.source === "string" && options.source.trim()
      ? options.source.trim().slice(0, 80)
      : "cloudflare-cron";
  const capturedAt = options.capturedAt ?? new Date().toISOString();

  log(`collecting Binance market data at ${capturedAt}`);

  const [spotExchangeInfo, usdmExchangeInfo, spot24hRaw, usdm24hRaw] = await Promise.all([
    fetchJsonFromCandidates(fetchImpl, SPOT_BASE_URLS, "/api/v3/exchangeInfo", null, {
      label: "Spot exchange info",
      retries: 1,
    }),
    fetchJsonFromCandidates(fetchImpl, USDM_BASE_URLS, "/fapi/v1/exchangeInfo", null, {
      label: "USD-M exchange info",
      retries: 1,
    }),
    fetchJsonFromCandidates(fetchImpl, SPOT_BASE_URLS, "/api/v3/ticker/24hr", null, {
      label: "Spot 24h tickers",
      retries: 2,
    }),
    fetchJsonFromCandidates(fetchImpl, USDM_BASE_URLS, "/fapi/v1/ticker/24hr", null, {
      label: "USD-M 24h tickers",
      retries: 2,
    }),
  ]);

  const activeSpotSymbols = createSpotTradingSymbolSet(spotExchangeInfo);
  const activeUsdmSymbols = createUsdmTradingSymbolSet(usdmExchangeInfo);
  const spot24h = filterTickerRows(spot24hRaw, activeSpotSymbols);
  const usdm24h = filterTickerRows(usdm24hRaw, activeUsdmSymbols);

  const spotSymbols = Array.from(
    new Set(
      (spot24h ?? [])
        .map((item) => item?.symbol)
        .filter(isUsdtTradingSymbol),
    ),
  ).sort();

  log(`spot symbols=${spotSymbols.length}, usdm tickers=${(usdm24h ?? []).length}`);

  const [spotRolling3d, spotRolling7d] = await Promise.all([
    fetchSpotRollingWindow(fetchImpl, "3d", spotSymbols),
    fetchSpotRollingWindow(fetchImpl, "7d", spotSymbols),
  ]);

  return {
    source,
    capturedAt,
    spot24h,
    spotRolling: {
      "3d": filterTickerRows(spotRolling3d, activeSpotSymbols),
      "7d": filterTickerRows(spotRolling7d, activeSpotSymbols),
    },
    usdm24h,
  };
}

async function fetchSpotRollingWindow(fetchImpl, windowKey, symbols) {
  const batches = chunk(symbols, SPOT_ROLLING_BATCH_SIZE);
  const pages = [];

  for (const batch of batches) {
    const page = await fetchJsonFromCandidates(
      fetchImpl,
      SPOT_BASE_URLS,
      "/api/v3/ticker",
      (url) => {
        url.searchParams.set("windowSize", windowKey);
        url.searchParams.set("type", "FULL");
        url.searchParams.set("symbols", JSON.stringify(batch));
      },
      {
        label: `Spot ${windowKey} rolling window`,
        retries: 2,
      },
    );

    pages.push(...(page ?? []));
  }

  return pages;
}

function filterTickerRows(items, activeSymbols = null) {
  if (!Array.isArray(items)) {
    return [];
  }

  return items.filter(
    (item) =>
      item?.symbol &&
      isUsdtTradingSymbol(item.symbol) &&
      (!activeSymbols || activeSymbols.has(item.symbol)),
  );
}

function isUsdtTradingSymbol(symbol) {
  return (
    typeof symbol === "string" &&
    symbol.endsWith(QUOTE_ASSET) &&
    !symbol.includes("_") &&
    symbol.length > QUOTE_ASSET.length
  );
}

function createSpotTradingSymbolSet(exchangeInfo) {
  const symbols = new Set();

  for (const item of exchangeInfo?.symbols ?? []) {
    if (item?.status !== "TRADING") {
      continue;
    }

    if (item?.quoteAsset !== QUOTE_ASSET || !isUsdtTradingSymbol(item.symbol)) {
      continue;
    }

    symbols.add(item.symbol);
  }

  return symbols;
}

function createUsdmTradingSymbolSet(exchangeInfo) {
  const symbols = new Set();

  for (const item of exchangeInfo?.symbols ?? []) {
    if (item?.status !== "TRADING") {
      continue;
    }

    if (item?.contractType !== "PERPETUAL") {
      continue;
    }

    if (item?.quoteAsset !== QUOTE_ASSET || !isUsdtTradingSymbol(item.symbol)) {
      continue;
    }

    symbols.add(item.symbol);
  }

  return symbols;
}

function chunk(list, size) {
  const result = [];
  for (let index = 0; index < list.length; index += size) {
    result.push(list.slice(index, index + size));
  }
  return result;
}

async function fetchJsonFromCandidates(fetchImpl, baseUrls, pathname, configureUrl, options = {}) {
  let lastError = null;

  for (const baseUrl of baseUrls) {
    const url = new URL(pathname, baseUrl);
    if (typeof configureUrl === "function") {
      configureUrl(url);
    }

    try {
      return await fetchJson(fetchImpl, url, {
        ...options,
        label: `${options.label ?? pathname} via ${baseUrl}`,
      });
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ?? new Error(`${pathname} failed on all base URLs`);
}

async function fetchJson(fetchImpl, url, options = {}) {
  const {
    timeoutMs = 30_000,
    retries = 1,
    retryDelayMs = 800,
    label = url.toString(),
  } = options;

  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetchImpl(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
        },
      });

      const rawText = await response.text();
      if (!response.ok) {
        throw new Error(`${label} failed with ${response.status}: ${rawText.slice(0, 240)}`);
      }

      try {
        return rawText ? JSON.parse(rawText) : null;
      } catch {
        throw new Error(`${label} returned invalid JSON: ${rawText.slice(0, 240) || "<empty>"}`);
      }
    } catch (error) {
      lastError = error;
      const shouldRetry =
        attempt < retries &&
        (error.name === "AbortError" || String(error.message ?? "").includes("fetch"));

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
