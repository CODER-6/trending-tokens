import { collectMarketSnapshot } from "../functions/_lib/binance-market-data.js";
import { ingestSnapshotBundle } from "../functions/_lib/snapshot-service.js";

export default {
  async scheduled(_controller, env, context) {
    context.waitUntil(runCollector(env, "cloudflare-cron"));
  },

  async fetch(request, env, context) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return json({
        ok: true,
        service: "collector",
        hint: "Use POST /run with the collector secret to trigger a manual sync.",
      });
    }

    if (request.method === "POST" && url.pathname === "/run") {
      const configuredSecret = getCollectorSecret(env);
      if (!configuredSecret || !isAuthorized(request, configuredSecret)) {
        return json(
          {
            error: "Unauthorized",
          },
          401,
        );
      }

      try {
        const result = await runCollector(env, "manual-trigger");
        return json(
          {
            ok: true,
            ...result,
          },
          200,
        );
      } catch (error) {
        return json(
          {
            error: "Collector run failed",
            message: error?.message ? String(error.message) : "Unknown error",
          },
          502,
        );
      }
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return json({
        ok: true,
        service: "collector",
        cron: "17 * * * *",
      });
    }

    return json(
      {
        error: "Not found",
      },
      404,
    );
  },
};

async function runCollector(env, source) {
  const payload = await collectMarketSnapshot({
    source,
    log: (message) => {
      console.log(`[collector] ${message}`);
    },
  });

  const result = await ingestSnapshotBundle(env, payload);
  console.log(`[collector] ingest complete: ${JSON.stringify(result.windows ?? {})}`);
  return result;
}

function getCollectorSecret(env) {
  const direct = typeof env?.COLLECTOR_SHARED_SECRET === "string" ? env.COLLECTOR_SHARED_SECRET.trim() : "";
  if (direct) {
    return direct;
  }

  const fallback = typeof env?.INGEST_SHARED_SECRET === "string" ? env.INGEST_SHARED_SECRET.trim() : "";
  return fallback;
}

function isAuthorized(request, configuredSecret) {
  const authHeader = request.headers.get("authorization") ?? "";
  const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  const headerSecret = request.headers.get("x-collector-secret") ?? "";
  return bearerToken === configuredSecret || headerSecret === configuredSecret;
}

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=UTF-8",
    },
  });
}
