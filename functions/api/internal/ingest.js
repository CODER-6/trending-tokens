import { ingestSnapshotBundle } from "../../_lib/snapshot-service.js";

export async function onRequestPost(context) {
  const configuredSecret =
    typeof context.env?.INGEST_SHARED_SECRET === "string"
      ? context.env.INGEST_SHARED_SECRET.trim()
      : "";

  if (!configuredSecret) {
    return json(
      {
        error: "INGEST_SHARED_SECRET is not configured",
      },
      500,
    );
  }

  if (!isAuthorized(context.request, configuredSecret)) {
    return json(
      {
        error: "Unauthorized",
      },
      401,
    );
  }

  let payload = null;
  try {
    payload = await context.request.json();
  } catch {
    return json(
      {
        error: "Invalid JSON body",
      },
      400,
    );
  }

  try {
    const result = await ingestSnapshotBundle(context.env, payload);
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
        error: "Failed to ingest snapshot bundle",
        message: error?.message ? String(error.message) : "Unknown error",
      },
      400,
    );
  }
}

export function onRequest() {
  return json(
    {
      error: "Method not allowed",
    },
    405,
    {
      Allow: "POST",
    },
  );
}

function isAuthorized(request, configuredSecret) {
  const authHeader = request.headers.get("authorization") ?? "";
  const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  const headerSecret = request.headers.get("x-ingest-secret") ?? "";
  return bearerToken === configuredSecret || headerSecret === configuredSecret;
}

function json(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=UTF-8",
      ...headers,
    },
  });
}
