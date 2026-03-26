import { collectMarketSnapshot } from "../functions/_lib/binance-market-data.js";

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
  const payload = await collectMarketSnapshot({
    source: "manual-push",
    log: (message) => {
      console.log(`[sync] ${message}`);
    },
  });

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

main().catch((error) => {
  console.error(`[sync] ${error?.message ? String(error.message) : error}`);
  process.exitCode = 1;
});
