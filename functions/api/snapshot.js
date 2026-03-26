import { getSnapshot, isSupportedWindow } from "../_lib/snapshot-service.js";

export async function onRequestGet(context) {
  const requestUrl = new URL(context.request.url);
  const windowKey = requestUrl.searchParams.get("window") ?? "1d";
  const force = requestUrl.searchParams.get("refresh") === "1";

  if (!isSupportedWindow(windowKey)) {
    return json(
      {
        error: `Unsupported window: ${windowKey}`,
      },
      400,
    );
  }

  try {
    const payload = await getSnapshot(context.env, windowKey, { force });

    return json(payload, 200, {
      "Cache-Control": "no-store",
      "X-Snapshot-Cache": payload.backend?.cacheStatus ?? "unknown",
      "X-Snapshot-Storage": payload.backend?.storage ?? "unknown",
    });
  } catch (error) {
    const message = error?.message ? String(error.message) : "Unknown error";

    return json(
      {
        error: "Failed to load snapshot",
        message,
      },
      getStatusCode(message),
      {
        "Cache-Control": "no-store",
      },
    );
  }
}

function getStatusCode(message) {
  if (message.includes("Unsupported window")) {
    return 400;
  }

  if (message.includes("No ingested snapshot available yet")) {
    return 503;
  }

  if (message.includes("D1 is not configured")) {
    return 500;
  }

  return 502;
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
