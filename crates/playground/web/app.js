"use strict";

// The matching core as WebAssembly: plain functions of numbers (crates/playground).
const LEVELS = 12;
const FIELDS = 3;
const ROUNDS = 5;
// Commands applied per animation frame, by the speed slider's position.
const SPEEDS = [2, 20, 200, 2_000, 20_000, 100_000];

const $ = (id) => document.getElementById(id);
const pause = () => new Promise((resolve) => setTimeout(resolve, 0));
const grouped = (n) => Math.round(n).toLocaleString("en-US");
const hex = (high, low) =>
  "0x" + (high >>> 0).toString(16).padStart(8, "0") + (low >>> 0).toString(16).padStart(8, "0");

function rate(perSecond) {
  if (perSecond >= 1e6) return `${(perSecond / 1e6).toFixed(1)}M`;
  if (perSecond >= 1e3) return `${(perSecond / 1e3).toFixed(0)}k`;
  return `${Math.round(perSecond)}`;
}

let engine = null;

async function load() {
  const response = await fetch("playground.wasm");
  const bytes = await response.arrayBuffer();
  $("size").textContent = `${Math.round(bytes.byteLength / 1024)} KB`;
  const { instance } = await WebAssembly.instantiate(bytes, {});
  engine = instance.exports;
}

// ---------- The benchmark ----------

function status(text) {
  $("status").textContent = text;
}

async function benchmark() {
  const button = $("run");
  button.disabled = true;
  button.textContent = "Running…";
  $("check").className = "check";
  $("check").textContent = "";
  status("Recording the order flow: participants generate 700,000 commands as they trade…");
  await pause();
  engine.prepare();
  const times = [];
  for (let round = 1; round <= ROUNDS; round += 1) {
    status(`Round ${round} of ${ROUNDS}: warming a fresh book up, then timing 500,000 commands…`);
    await pause();
    engine.warm_up();
    const started = performance.now();
    const measured = engine.measure();
    const seconds = (performance.now() - started) / 1000;
    times.push(seconds / measured);
  }
  const sorted = [...times].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const perSecond = 1 / median;
  $("rate").textContent = rate(perSecond);
  $("per").textContent = `${Math.round(median * 1e9)} ns`;
  $("rounds").textContent = sorted.map((t) => rate(1 / t)).join(" · ");
  $("resting").textContent = grouped(engine.resting_orders());
  $("bar-browser").style.width = `${Math.min(100, (perSecond / 27.6e6) * 100)}%`;
  $("bar-browser-value").textContent = `${rate(perSecond)}/s`;

  const digest = hex(engine.digest_high(), engine.digest_low());
  const expected = hex(engine.expected_high(), engine.expected_low());
  $("digest").textContent = digest;
  const same = digest === expected;
  $("check").className = same ? "check ok" : "check bad";
  $("check").textContent = same ? "✓ identical to native builds" : `✗ native builds get ${expected}`;
  status(`Median of ${ROUNDS} rounds. Run it again, or in another browser, and compare.`);
  button.textContent = "Run again";
  button.disabled = false;
}

// ---------- The live book ----------

function makeRows(element) {
  const rows = [];
  for (let i = 0; i < LEVELS; i += 1) {
    const row = document.createElement("div");
    row.className = "row";
    const bar = document.createElement("i");
    const price = document.createElement("span");
    const qty = document.createElement("span");
    const orders = document.createElement("span");
    row.append(bar, price, qty, orders);
    element.append(row);
    rows.push({ row, bar, price, qty, orders });
  }
  return rows;
}

const askRows = makeRows($("asks"));
const bidRows = makeRows($("bids"));
let speed = SPEEDS[1];
let lastFrame = performance.now();
let lastCommands = 0;
let smoothed = 0;

function show(rows, levels, max, reverse) {
  rows.forEach((cells, i) => {
    const level = levels[reverse ? LEVELS - 1 - i : i];
    if (!level) {
      cells.row.classList.add("empty");
      cells.price.textContent = cells.qty.textContent = cells.orders.textContent = "";
      cells.bar.style.width = "0";
      return;
    }
    cells.row.classList.remove("empty");
    cells.price.textContent = grouped(level.price);
    cells.qty.textContent = grouped(level.qty);
    cells.orders.textContent = level.orders;
    cells.bar.style.width = `${(100 * level.qty) / max}%`;
  });
}

function frame(now) {
  const commands = engine.live_step(speed);
  const depth = new Float64Array(engine.memory.buffer, engine.live_depth(), 2 * LEVELS * FIELDS);
  const side = (half) => {
    const levels = [];
    for (let i = 0; i < LEVELS; i += 1) {
      const at = (half * LEVELS + i) * FIELDS;
      if (depth[at] === 0) break;
      levels.push({ price: depth[at], qty: depth[at + 1], orders: depth[at + 2] });
    }
    return levels;
  };
  const bids = side(0);
  const asks = side(1);
  const max = Math.max(1, ...bids.map((l) => l.qty), ...asks.map((l) => l.qty));
  show(askRows, asks, max, true);
  show(bidRows, bids, max, false);
  const last = engine.live_last_price();
  $("last").textContent = last ? grouped(last) : "—";
  $("spread").textContent = bids.length && asks.length ? `spread ${grouped(asks[0].price - bids[0].price)}` : "";

  const elapsed = (now - lastFrame) / 1000;
  if (elapsed > 0) {
    const instant = (commands - lastCommands) / elapsed;
    smoothed = smoothed ? smoothed * 0.9 + instant * 0.1 : instant;
  }
  lastFrame = now;
  lastCommands = commands;
  $("live-rate").textContent = rate(smoothed);
  $("live-commands").textContent = grouped(commands);
  $("live-trades").textContent = grouped(engine.live_trades());
  $("live-orders").textContent = grouped(engine.live_orders());
  requestAnimationFrame(frame);
}

$("speed").addEventListener("input", (event) => {
  speed = SPEEDS[Number(event.target.value)];
  $("speed-value").textContent = grouped(speed);
});

$("run").addEventListener("click", () => {
  benchmark().catch((error) => {
    status(`The benchmark stopped: ${error.message}`);
    $("run").disabled = false;
    $("run").textContent = "Try again";
  });
});

load()
  .then(() => {
    $("run").disabled = false;
    $("run").textContent = "Run the benchmark";
    requestAnimationFrame(frame);
  })
  .catch((error) => {
    status(`Your browser could not load the engine: ${error.message}`);
    $("run").textContent = "Unavailable";
  });
