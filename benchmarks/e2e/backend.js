#!/usr/bin/env node

const http = require("node:http");

const host = process.env.BENCH_HOST || "127.0.0.1";
const port = Number.parseInt(process.env.BENCH_PORT || "18080", 10);
const maximumBodySize = 16 * 1024 * 1024;
const bodies = new Map();

function bodyOfSize(size) {
  let body = bodies.get(size);
  if (!body) {
    body = Buffer.alloc(size, "x");
    bodies.set(size, body);
  }
  return body;
}

const server = http.createServer((request, response) => {
  const match = /^\/bytes\/(\d+)$/.exec(request.url);
  if (!match) {
    response.writeHead(request.url === "/health" ? 200 : 404, {
      "content-length": "0",
    });
    response.end();
    return;
  }

  const size = Number.parseInt(match[1], 10);
  if (!Number.isSafeInteger(size) || size < 0 || size > maximumBodySize) {
    response.writeHead(400, { "content-length": "0" });
    response.end();
    return;
  }

  request.resume();
  request.on("end", () => {
    const body = bodyOfSize(size);
    response.writeHead(200, {
      "content-type": "application/octet-stream",
      "content-length": String(body.length),
    });
    response.end(body);
  });
});

server.keepAliveTimeout = 60_000;
server.listen(port, host, () => {
  console.log(`benchmark backend listening on http://${host}:${port}`);
});
