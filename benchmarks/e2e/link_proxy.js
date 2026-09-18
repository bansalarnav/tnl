#!/usr/bin/env node

const net = require("node:net");
const { performance } = require("node:perf_hooks");

const profiles = {
  loopback: { delayMs: 0, jitterMs: 0, connectDelayMs: 0, bandwidthMbps: 0 },
  lan: { delayMs: 0.5, jitterMs: 0.1, connectDelayMs: 1, bandwidthMbps: 1000 },
  wan30: { delayMs: 15, jitterMs: 2, connectDelayMs: 30, bandwidthMbps: 200 },
  wan100: { delayMs: 50, jitterMs: 5, connectDelayMs: 100, bandwidthMbps: 50 },
};

const profileName = process.argv[2] || "wan30";
const profile = profiles[profileName];
if (!profile) {
  console.error(`unknown link profile ${profileName}; expected ${Object.keys(profiles).join(", ")}`);
  process.exit(2);
}

function numberSetting(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number`);
  }
  return value;
}

const settings = {
  listenHost: process.env.LINK_LISTEN_HOST || "127.0.0.1",
  listenPort: numberSetting("LINK_LISTEN_PORT", 9443),
  targetHost: process.env.LINK_TARGET_HOST || "127.0.0.1",
  targetPort: numberSetting("LINK_TARGET_PORT", 8443),
  delayMs: numberSetting("LINK_DELAY_MS", profile.delayMs),
  jitterMs: numberSetting("LINK_JITTER_MS", profile.jitterMs),
  connectDelayMs: numberSetting("LINK_CONNECT_DELAY_MS", profile.connectDelayMs),
  bandwidthMbps: numberSetting("LINK_BANDWIDTH_MBPS", profile.bandwidthMbps),
  maxQueueBytes: numberSetting("LINK_MAX_QUEUE_BYTES", 64 * 1024 * 1024),
};

if (!Number.isInteger(settings.listenPort) || settings.listenPort > 65535) {
  throw new Error("LINK_LISTEN_PORT must be an integer between 0 and 65535");
}
if (!Number.isInteger(settings.targetPort) || settings.targetPort < 1 || settings.targetPort > 65535) {
  throw new Error("LINK_TARGET_PORT must be an integer between 1 and 65535");
}

function jitter() {
  if (settings.jitterMs === 0) return 0;
  const first = Math.max(Number.MIN_VALUE, Math.random());
  const second = Math.random();
  return Math.sqrt(-2 * Math.log(first)) * Math.cos(2 * Math.PI * second) * settings.jitterMs;
}

function delayedPipe(source, destination, firstChunkExtraMs, link) {
  const queue = [];
  const resumeBelowBytes = settings.maxQueueBytes / 2;
  let queuedBytes = 0;
  let lastDeliveryAt = 0;
  let timer = null;
  let blocked = false;
  let firstChunk = true;

  function schedule() {
    if (timer !== null || blocked || queue.length === 0) return;
    const waitMs = Math.max(0, queue[0].deliveryAt - performance.now());
    timer = setTimeout(drain, waitMs);
  }

  function drain() {
    timer = null;
    const now = performance.now();
    while (!blocked && queue.length > 0 && queue[0].deliveryAt <= now + 0.1) {
      const item = queue.shift();
      if (item.chunk === null) {
        destination.end();
        continue;
      }
      queuedBytes -= item.chunk.length;
      blocked = !destination.write(item.chunk);
      if (source.isPaused() && queuedBytes <= resumeBelowBytes) source.resume();
    }
    if (blocked) {
      destination.once("drain", () => {
        blocked = false;
        drain();
      });
    } else {
      schedule();
    }
  }

  function enqueue(chunk) {
    const now = performance.now();
    const serializationMs = settings.bandwidthMbps === 0
      ? 0
      : chunk.length * 8 / (settings.bandwidthMbps * 1_000_000) * 1000;
    const transmissionStart = Math.max(now, link.transmissionFreeAt);
    link.transmissionFreeAt = transmissionStart + serializationMs;
    const extraDelay = firstChunk ? firstChunkExtraMs : 0;
    firstChunk = false;
    const deliveryAt = Math.max(
      lastDeliveryAt,
      link.transmissionFreeAt + settings.delayMs + Math.max(-settings.delayMs, jitter()) + extraDelay,
    );
    lastDeliveryAt = deliveryAt;
    queue.push({ chunk, deliveryAt });
    queuedBytes += chunk.length;
    if (queuedBytes >= settings.maxQueueBytes) source.pause();
    schedule();
  }

  source.on("data", enqueue);
  source.on("end", () => {
    const deliveryAt = Math.max(lastDeliveryAt, performance.now() + settings.delayMs);
    queue.push({ chunk: null, deliveryAt });
    schedule();
  });
}

const uploadLink = { transmissionFreeAt: 0 };
const downloadLink = { transmissionFreeAt: 0 };
const sockets = new Set();

const server = net.createServer({ allowHalfOpen: true }, (client) => {
  client.setNoDelay(true);
  const upstream = net.createConnection({
    allowHalfOpen: true,
    host: settings.targetHost,
    port: settings.targetPort,
  });
  upstream.setNoDelay(true);
  sockets.add(client);
  sockets.add(upstream);
  client.once("close", () => sockets.delete(client));
  upstream.once("close", () => sockets.delete(upstream));

  const closeBoth = () => {
    client.destroy();
    upstream.destroy();
  };
  client.on("error", closeBoth);
  upstream.on("error", closeBoth);
  upstream.on("connect", () => {
    delayedPipe(client, upstream, settings.connectDelayMs, uploadLink);
    delayedPipe(upstream, client, 0, downloadLink);
  });
});

server.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});

server.listen(settings.listenPort, settings.listenHost, () => {
  const address = server.address();
  console.log(JSON.stringify({ profile: profileName, ...settings, listenPort: address.port }));
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    for (const socket of sockets) socket.destroy();
    server.close(() => process.exit(0));
  });
}
