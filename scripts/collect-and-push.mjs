const QUOTE_ASSET = "USDT";
const SPOT_BASE_URL = "https://api.binance.com";
const USDM_BASE_URL = "https://fapi.binance.com";
const SPOT_ROLLING_BATCH_SIZE = 50;

const requiredEnv = {
  INGEST_BASE_URL: process.env.INGEST_BASE_URL,
  INGEST_SHARED_SECRET: process.env.INGEST_SHARED_SECRET,
};

for (const [key, value] of Object.entries(requiredEnv)) {
  if (!value || !value.trim()) {
    throw new Error(`${key} is required`);
  }
}

const ingestUrl = new URL("/api/internal/ingest", requiredEnv.INGEST_BASE_URL).toString();

async function main() {
  const capturedAt = new Date().toISOString();

  console.log(`[sync] collecting Binance market data at ${capturedAt}`);
  const spot24h = await fetchJson(new URL("/api/v3/ticker/24hr", SPOT_BASE_URL), {
    label: "Spot 24h tickers",
    retries: 2,
  });
  const usdm24h = await fetchJson(new URL("/fapi/v1/ticker/24hr", USDM_BASE_URL), {
    label: "USD-M 24h tickers",
    retries: 2,
  });

  const spotSymbols = Array.from(
    new Set(
      (spot24h ?? [])
        .map((item) => item?.symbol)
        .filter(isUsdtTradingSymbol),
    ),
  ).sort();

  console.log(`[sync] spot symbols=${spotSymbols.length}, usdm tickers=${(usdm24h ?? []).length}`);
  const [spotRolling3d, spotRolling7d] = await Promise.all([
    fetchSpotRollingWindow("3d", spotSymbols),
    fetchSpotRollingWindow("7d", spotSymbols),
  ]);

  const payload = {
    source: "github-actions",
    capturedAt,
    spot24h: filterTickerRows(spot24h),
    spotRolling: {
      "3d": filterTickerRows(spotRolling3d),
      "7d": filterTickerRows(spotRolling7d),
    },
    usdm24h: filterTickerRows(usdm24h),
  };

  const response = await fetch(ingestUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${requiredEnv.INGEST_SHARED_SECRET.trim()}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const rawText = await response.text();
  let result = null;

  try {
    result = rawText ? JSON.parse(rawText) : null;
  } catch {
    throw new Error(`Ingest endpoint returned invalid JSON: ${rawText.slice(0, 240) || "<empty>"}`);
  }

  if (!response.ok) {
    throw new Error(result?.message || result?.error || `Ingest failed with ${response.status}`);
  }

  console.log(`[sync] ingest complete: ${JSON.stringify(result.windows ?? {})}`);
}

async function fetchSpotRollingWindow(windowKey, symbols) {
  const batches = chunk(symbols, SPOT_ROLLING_BATCH_SIZE);
  const pages = [];

  for (const batch of batches) {
    const url = new URL("/api/v3/ticker", SPOT_BASE_URL);
    url.searchParams.set("windowSize", windowKey);
    url.searchParams.set("type", "FULL");
    url.searchParams.set("symbols", JSON.stringify(batch));

    const page = await fetchJson(url, {
      label: `Spot ${windowKey} rolling window`,
      retries: 2,
    });

    pages.push(...(page ?? []));
  }

  return pages;
}

function filterTickerRows(items) {
  if (!Array.isArray(items)) {
    return [];
  }

  return items.filter((item) => item?.symbol && isUsdtTradingSymbol(item.symbol));
}

function isUsdtTradingSymbol(symbol) {
  return (
    typeof symbol === "string" &&
    symbol.endsWith(QUOTE_ASSET) &&
    !symbol.includes("_") &&
    symbol.length > QUOTE_ASSET.length
  );
}

function chunk(list, size) {
  const result = [];
  for (let index = 0; index < list.length; index += size) {
    result.push(list.slice(index, index + size));
  }
  return result;
}

async function fetchJson(url, options = {}) {
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
      const response = await fetch(url, {
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

main().catch((error) => {
  console.error(`[sync] ${error?.message ? String(error.message) : error}`);
  process.exitCode = 1;
});
