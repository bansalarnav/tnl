#!/usr/bin/env node

const fs = require("node:fs");

if (process.argv.length !== 4) {
  console.error("usage: summarize.js SUMMARY_CSV AGGREGATE_CSV");
  process.exit(2);
}

function parseCsvLine(line) {
  const values = [];
  let value = "";
  let quoted = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (quoted) {
      if (character === '"' && line[index + 1] === '"') {
        value += '"';
        index += 1;
      } else if (character === '"') {
        quoted = false;
      } else {
        value += character;
      }
    } else if (character === '"') {
      quoted = true;
    } else if (character === ",") {
      values.push(value);
      value = "";
    } else {
      value += character;
    }
  }
  values.push(value);
  return values;
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function csv(value) {
  const text = String(value);
  if (!/[",\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

const [summaryPath, aggregatePath] = process.argv.slice(2);
const lines = fs.readFileSync(summaryPath, "utf8").trim().split("\n");
const columns = parseCsvLine(lines.shift());
const rows = lines.map((line) => {
  const values = parseCsvLine(line);
  return Object.fromEntries(columns.map((column, index) => [column, values[index]]));
});

const groups = new Map();
for (const row of rows) {
  const key = `${row.path}\0${row.case}`;
  const group = groups.get(key) || [];
  group.push(row);
  groups.set(key, group);
}

const medianColumns = [
  "responses",
  "elapsed_seconds",
  "requests_per_sec",
  "mean_ms",
  "p50_ms",
  "p95_ms",
  "p99_ms",
  "ttfb_mean_ms",
  "ttfb_p50_ms",
  "ttfb_p95_ms",
  "ttfb_p99_ms",
  "success_rate",
  "response_mbps",
  "request_mbps",
  "total_payload_mbps",
];
const outputColumns = [
  "path",
  "case",
  "response_bytes",
  "request_bytes",
  "concurrency",
  "keepalive",
  "repetitions",
  "errors_total",
  "success_rate_min",
  ...medianColumns.map((column) => `${column}_median`),
];
const output = [outputColumns.join(",")];

for (const group of groups.values()) {
  const first = group[0];
  const fixed = [
    first.path,
    first.case,
    first.response_bytes,
    first.request_bytes,
    first.concurrency,
    first.keepalive,
    group.length,
    group.reduce((total, row) => total + Number(row.errors), 0),
    Math.min(...group.map((row) => Number(row.success_rate))),
  ];
  const medians = medianColumns.map((column) =>
    median(group.map((row) => Number(row[column]))),
  );
  output.push([...fixed, ...medians].map(csv).join(","));
}

fs.writeFileSync(aggregatePath, `${output.join("\n")}\n`);
