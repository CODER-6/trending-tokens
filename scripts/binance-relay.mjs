import http from "node:http";

const host = process.env.BINANCE_RELAY_HOST || "127.0.0.1";
const port = Number(process.env.BINANCE_RELAY_PORT || 8789);

const routes = [
  { prefix: "/spot", origin: "https://api.binance.com" },
  { prefix: "/fapi", origin: "https://fapi.binance.com" },
  { prefix: "/data-api", origin: "https://data-api.binance.vision" },
];

const server = http.createServer(async (request, response) => {
  if (!request.url) {
    writeJson(response, 400, { error: "Missing request URL" });
    return;
  }

  if (request.method !== "GET" && request.method !== "HEAD") {
    writeJson(response, 405, { error: "Only GET and HEAD are supported" });
    return;
  }

  const requestUrl = new URL(request.url, `http://${request.headers.host || `${host}:${port}`}`);
  const route = routes.find((entry) => requestUrl.pathname.startsWith(entry.prefix));

  if (!route) {
    writeJson(response, 404, { error: `Unsupported relay path: ${requestUrl.pathname}` });
    return;
  }

  const upstreamPath = requestUrl.pathname.slice(route.prefix.length) || "/";
  const upstreamUrl = new URL(upstreamPath + requestUrl.search, route.origin);

  try {
    const upstreamResponse = await fetch(upstreamUrl, {
      method: request.method,
      headers: {
        Accept: "application/json",
        "User-Agent": "crypto-hot-coin-relay/1.0",
      },
    });

    const buffer = Buffer.from(await upstreamResponse.arrayBuffer());
    response.writeHead(upstreamResponse.status, {
      "content-type": upstreamResponse.headers.get("content-type") || "application/octet-stream",
      "cache-control": upstreamResponse.headers.get("cache-control") || "no-store",
      "access-control-allow-origin": "*",
    });
    response.end(request.method === "HEAD" ? undefined : buffer);
  } catch (error) {
    writeJson(response, 502, {
      error: "Relay upstream request failed",
      message: error instanceof Error ? error.message : String(error),
      upstreamUrl: upstreamUrl.toString(),
    });
  }
});

server.listen(port, host, () => {
  console.log(`Binance relay ready on http://${host}:${port}`);
});

function writeJson(response, status, payload) {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
  });
  response.end(JSON.stringify(payload));
}
