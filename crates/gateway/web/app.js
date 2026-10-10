"use strict";

// What is traded, as the server describes it. Prices are integer ticks of
// 10^-price_decimals of the quote currency, quantities integer lots of 10^-lot_decimals of
// the base asset, and cash is ticks times lots.
const market = {
  symbol: "DEMO/USD",
  base: "DEMO",
  quote: "USD",
  price_decimals: 2,
  lot_decimals: 0,
  source: null,
  source_url: null,
};
const dollars = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const whole = new Intl.NumberFormat("en-US");
const money = (units) => dollars.format(units / 10 ** (market.price_decimals + market.lot_decimals));
const price = (ticks) => (ticks / 10 ** market.price_decimals).toFixed(market.price_decimals);
const count = (n) => whole.format(n);
// A quantity, with at most four decimals.
const size = (lots) => {
  const decimals = Math.min(market.lot_decimals, 4);
  return (lots / 10 ** market.lot_decimals).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};
// A number typed with a decimal point or comma, in units of 10^-decimals.
const scaled = (text, decimals) => Math.round(Number(String(text).trim().replace(",", ".")) * 10 ** decimals);
const ticksOf = (text) => scaled(text, market.price_decimals);
const lotsOf = (text) => scaled(text, market.lot_decimals);
// A quantity as it is typed into a field.
const plain = (lots) => String(lots / 10 ** market.lot_decimals);
const $ = (id) => document.getElementById(id);
const two = (n) => String(n).padStart(2, "0");
const timeOf = (ms) => {
  const at = new Date(ms);
  return `${two(at.getHours())}:${two(at.getMinutes())}:${two(at.getSeconds())}`;
};

// Levels shown on each side of the book.
const DEPTH = 10;
// The server's candles, in seconds, and how far back it keeps them.
const INTERVAL = 5;
const KEPT = 3_600;

const state = {
  socket: null,
  connected: false,
  account: null,
  side: "buy",
  type: "limit",
  bids: new Map(), // price -> {qty, orders}
  asks: new Map(),
  trades: [],
  last: null,
  direction: 0,
  wallet: null,
  baseline: null, // what the account started with: profit is measured from it
  requests: new Map(), // client ref -> the order as sent
  orders: new Map(), // order id -> {id, side, price, qty, leaves, time, tif}
  fills: [],
  nextRef: Date.now() % 1_000_000_000,
  retry: 500,
  offset: 0, // the server's clock minus ours, in milliseconds
  statsAt: 0,
  queues: { buy: [], sell: [] },
  leaders: null,
  rank: null,
};

const now = () => Date.now() + state.offset;

// ---------- Storage that may not be there ----------

function load(key) {
  try {
    return JSON.parse(localStorage.getItem(key) || "null");
  } catch {
    return null;
  }
}

function save(key, value) {
  try {
    if (value === null) localStorage.removeItem(key);
    else localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // A private window: the account lasts as long as the page.
  }
}

// ---------- Drawing, at most once a frame ----------

const dirty = new Set();
let framePending = false;
function schedule(...parts) {
  for (const part of parts) dirty.add(part);
  if (framePending) return;
  framePending = true;
  requestAnimationFrame(() => {
    framePending = false;
    const parts = [...dirty];
    dirty.clear();
    for (const part of parts) renderers[part]();
  });
}

// ---------- Connection ----------

function send(message) {
  if (state.socket && state.socket.readyState === WebSocket.OPEN) {
    state.socket.send(JSON.stringify(message));
  }
}

function setConnection(kind, text) {
  $("connection").dataset.state = kind;
  $("connection-text").textContent = text;
  state.connected = kind === "live";
  schedule("ticket");
}

function connect() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/ws`);
  state.socket = socket;
  socket.onopen = () => {
    state.retry = 500;
    setConnection("connecting", "Signing in");
    const saved = load("exchange-account");
    if (saved) send({ type: "login", account: saved.account, token: saved.token });
    else send({ type: "register" });
  };
  socket.onmessage = (event) => receive(JSON.parse(event.data));
  socket.onclose = () => {
    setConnection("down", state.elsewhere ? "Open in another tab" : "Reconnecting");
    state.orders.clear();
    state.requests.clear();
    schedule("orders", "book");
    setTimeout(connect, state.retry);
    state.retry = Math.min(state.retry * 2, 10_000);
  };
}

const REASONS = {
  too_many_orders: "You have as many open orders as a paper account may.",
  throttled: "Too many messages at once. Slow down a little.",
  unavailable: "The exchange is not taking orders right now.",
  insufficient_funds: "Your paper account does not cover this order.",
  not_allowed: "Paper accounts place limit orders and cancel them.",
  price_out_of_range: "That price is outside the allowed range.",
  price_outside_protection: "That price is too far from the market.",
  price_outside_band: "That price is too far from the last trade.",
  post_only_would_cross: "A post-only order would have traded at once.",
  invalid_quantity: "That amount is not allowed.",
  unknown_order: "That order is no longer open.",
  book_full: "The order book is full.",
  trading_halted: "Trading is halted.",
  auction_call: "Only limit orders are taken during the auction call.",
  market_closed: "The market is closed.",
  already_logged_in: "This account is open in another tab.",
};
const reason = (code) => {
  const text = REASONS[code] || code.replaceAll("_", " ");
  return text.charAt(0).toUpperCase() + text.slice(1);
};

function receive(message) {
  switch (message.type) {
    case "registered":
      save("exchange-account", { account: message.account, token: message.token });
      send({ type: "login", account: message.account, token: message.token });
      break;
    case "login_accepted":
      state.account = message.account;
      state.elsewhere = false;
      setConnection("live", "Live");
      $("account").hidden = false;
      $("account").replaceChildren("Account", bold(`#${message.account}`));
      $("account-id").textContent = `#${message.account}`;
      send({ type: "subscribe" });
      break;
    case "login_rejected":
      // An account the exchange no longer knows, or one open in another tab.
      // The other tab may be closing: this one keeps trying, and says so once.
      if (message.reason === "bad_credentials") {
        save("exchange-account", null);
        send({ type: "register" });
      } else if (message.reason === "already_logged_in") {
        if (!state.elsewhere) toast("Open in another tab", "This tab takes over when that one closes.", "error");
        state.elsewhere = true;
      } else {
        toast("Could not sign in", reason(message.reason), "error");
      }
      break;
    case "book":
      state.bids.clear();
      state.asks.clear();
      schedule("book");
      break;
    case "level": {
      const side = message.side === "buy" ? state.bids : state.asks;
      if (message.orders === 0) side.delete(message.price);
      else side.set(message.price, { qty: message.qty, orders: message.orders });
      schedule("book", "ticket");
      if (chart.view === "depth") schedule("chart");
      break;
    }
    case "history":
      chart.base = message.candles.map((candle) => ({ ...candle }));
      if (state.last === null && chart.base.length) state.last = chart.base[chart.base.length - 1].c;
      schedule("chart", "ticker", "wallet");
      break;
    case "trade":
      trade(message);
      break;
    case "report":
      report(message);
      break;
    case "balance":
      balance(message);
      break;
    case "reject": {
      const request = state.requests.get(message.ref);
      state.requests.delete(message.ref);
      toast(request ? `${sideName(request.side)} order refused` : "Refused", reason(message.reason), "error");
      break;
    }
    case "logout":
      if (message.reason !== "requested") toast("Signed out", reason(message.reason), "error");
      break;
    case "error":
      if (message.message === "no accounts are left") {
        toast("The demo is full", "Every paper account is taken. Try again later.", "error");
      } else toast("Something went wrong", message.message, "error");
      break;
    case "stats":
      stats(message);
      break;
    case "queues":
      state.queues = { buy: unpack(message.buy), sell: unpack(message.sell) };
      schedule("book", "orders");
      break;
    case "log":
      engineLog(message);
      break;
    case "market":
      setMarket(message);
      break;
    case "started":
      state.started = message;
      renderStarted();
      break;
    case "stats_history":
      engine.heat = message.stats.slice(-SECONDS).map(heatOf);
      schedule("engine");
      break;
    case "leaders":
      state.leaders = message;
      schedule("leaders");
      break;
    case "rank":
      state.rank = message;
      schedule("leaders");
      break;
    default:
      break;
  }
}

const bold = (text) => {
  const element = document.createElement("b");
  element.textContent = text;
  return element;
};
const sideName = (side) => (side === "buy" ? "Buy" : "Sell");

// ---------- Market data ----------

function trade(message) {
  state.direction = state.last === null ? 0 : Math.sign(message.price - state.last) || state.direction;
  state.last = message.price;
  state.trades.unshift({
    time: now(),
    price: message.price,
    qty: message.qty,
    side: message.side,
    shown: false,
  });
  if (state.trades.length > 60) state.trades.length = 60;
  record(message.price, message.qty);
  schedule("trades", "chart", "ticker", "wallet", "book");
}

// ---------- Your orders ----------

function report(message) {
  const known = state.orders.get(message.id);
  switch (message.kind) {
    case "accepted": {
      const request = state.requests.get(message.ref);
      state.requests.delete(message.ref);
      if (request) {
        state.orders.set(message.id, { id: message.id, ...request, leaves: request.qty });
        if (request.tif !== "ioc") remember(message.id, request);
      }
      break;
    }
    case "rested":
      if (known) known.leaves = message.qty;
      else {
        // An order from before this page: its size and time, if this browser placed it.
        const placed = recall(message.id);
        state.orders.set(message.id, {
          id: message.id,
          side: message.side,
          price: message.price,
          qty: placed ? placed.qty : null,
          leaves: message.qty,
          time: placed ? placed.time : null,
          tif: placed ? placed.tif : "gtc",
        });
      }
      break;
    case "fill":
      state.fills.unshift({ time: now(), side: message.side, price: message.price, qty: message.qty });
      if (state.fills.length > 100) state.fills.length = 100;
      if (message.leaves === 0) {
        state.orders.delete(message.id);
        if (known && known.tif !== "ioc") remember(message.id, null);
      } else if (known) known.leaves = message.leaves;
      filled(message);
      break;
    case "cancelled":
      state.orders.delete(message.id);
      if (known && known.tif !== "ioc") remember(message.id, null);
      // The rest of an immediate order is cancelled as a matter of course.
      if (known && known.tif !== "ioc") {
        toast("Order cancelled", `${sideName(known.side)} ${size(known.leaves)} ${market.base} at ${price(known.price)}`);
      }
      break;
    case "rejected":
      state.orders.delete(message.id);
      toast("Order refused by the book", reason(message.reason), "error");
      break;
    case "phase_changed":
      phase(message.phase);
      break;
    default:
      break;
  }
  schedule("orders", "book");
}

// What this browser placed, so that an order told again after a reload keeps its size
// and time. Only the latest hundred are kept.
function remember(id, order) {
  const saved = load("exchange-placed");
  const placed = saved && saved.account === state.account ? saved.placed : {};
  if (order) placed[id] = { qty: order.qty, time: order.time, tif: order.tif };
  else delete placed[id];
  const ids = Object.keys(placed).map(Number).sort((a, b) => a - b);
  for (const old of ids.slice(0, Math.max(0, ids.length - 100))) delete placed[old];
  save("exchange-placed", { account: state.account, placed });
}

function recall(id) {
  const saved = load("exchange-placed");
  return saved && saved.account === state.account ? saved.placed[id] || null : null;
}

// Fills of one order that come together are told as one.
const pendingFills = new Map();
let fillTimer = null;
function filled(message) {
  const sum = pendingFills.get(message.id) || { side: message.side, qty: 0, value: 0 };
  sum.qty += message.qty;
  sum.value += message.qty * message.price;
  pendingFills.set(message.id, sum);
  if (fillTimer !== null) return;
  fillTimer = setTimeout(() => {
    for (const [id, fill] of pendingFills) {
      const average = price(Math.round(fill.value / fill.qty));
      const verb = fill.side === "buy" ? "Bought" : "Sold";
      toast(`${verb} ${size(fill.qty)} ${market.base}`, `at ${average} on average · order #${id}`, fill.side);
    }
    pendingFills.clear();
    fillTimer = null;
  }, 250);
}

function balance(message) {
  state.wallet = message;
  if (state.baseline === null || state.baseline.account !== state.account) {
    const saved = load("exchange-baseline");
    if (saved && saved.account === state.account) state.baseline = saved;
    else {
      // What a new account starts with, or what an older one had when first seen here.
      state.baseline = { account: state.account, cash: message.cash, position: message.position };
      save("exchange-baseline", state.baseline);
    }
  }
  schedule("wallet", "ticket");
}

const PHASES = { continuous: "Open", auction: "Auction", halted: "Halted", closed: "Closed" };
function phase(name) {
  const element = $("phase");
  element.textContent = PHASES[name] || name;
  element.className = `phase ${name}`;
}

// ---------- Order book ----------

function makeRows(element, side) {
  const slots = [];
  for (let index = 0; index < DEPTH; index += 1) {
    const row = document.createElement("div");
    row.className = "level empty-row";
    const bar = document.createElement("i");
    bar.className = "bar";
    const at = document.createElement("span");
    at.className = "price";
    const qty = document.createElement("span");
    qty.className = "qty";
    const total = document.createElement("span");
    total.className = "total";
    const blocks = document.createElement("span");
    blocks.className = "blocks";
    row.append(bar, at, qty, total, blocks);
    const slot = {
      row, bar, at, qty, total, blocks, blockMap: new Map(), queuePrice: null, price: null, size: null,
    };
    // Clicking a level sets up the order that would trade with it.
    row.addEventListener("click", () => {
      if (slot.price !== null) pick(slot.price, side === "ask" ? "buy" : "sell");
    });
    element.append(row);
    slots.push(slot);
  }
  return slots;
}

const askSlots = makeRows($("asks"), "ask");
const bidSlots = makeRows($("bids"), "bid");

function show(slot, level, total, max, mine) {
  if (!level) {
    if (slot.price !== null) {
      slot.row.className = "level empty-row";
      slot.row.removeAttribute("title");
      slot.at.textContent = slot.qty.textContent = slot.total.textContent = "";
      slot.at.className = "price";
      slot.bar.style.width = "0";
      slot.price = slot.size = null;
    }
    return;
  }
  const [at, { qty, orders }] = level;
  const changed = slot.price === at && slot.size !== qty;
  slot.at.textContent = price(at);
  slot.at.className = mine ? "price mine" : "price";
  slot.qty.textContent = size(qty);
  slot.total.textContent = size(total);
  slot.bar.style.width = `${(100 * total) / max}%`;
  slot.row.title = `${orders} order${orders === 1 ? "" : "s"} at ${price(at)}`;
  if (slot.price === null) slot.row.className = "level";
  if (changed) {
    slot.row.classList.remove("flash");
    void slot.row.offsetWidth;
    slot.row.classList.add("flash");
  }
  slot.price = at;
  slot.size = qty;
}

const sorted = (side) => {
  const levels = [...(side === "buy" ? state.bids : state.asks)];
  return levels.sort(side === "buy" ? (a, b) => b[0] - a[0] : (a, b) => a[0] - b[0]);
};

function renderBook() {
  const bids = sorted("buy").slice(0, DEPTH);
  const asks = sorted("sell").slice(0, DEPTH);
  const totals = (levels) => {
    let sum = 0;
    return levels.map(([, level]) => (sum += level.qty));
  };
  const bidTotals = totals(bids);
  const askTotals = totals(asks);
  const bidSum = bidTotals[bidTotals.length - 1] || 0;
  const askSum = askTotals[askTotals.length - 1] || 0;
  const max = Math.max(1, bidSum, askSum);
  const mine = (side) => new Set([...state.orders.values()].filter((o) => o.side === side).map((o) => o.price));
  const myBids = mine("buy");
  const myAsks = mine("sell");
  // The best ask sits at the bottom of its half, next to the spread.
  if (book.mode === "orders") renderQueues();
  else {
    askSlots.forEach((slot, index) => {
      const at = DEPTH - 1 - index;
      show(slot, asks[at], askTotals[at], max, asks[at] && myAsks.has(asks[at][0]));
    });
    bidSlots.forEach((slot, index) => {
      show(slot, bids[index], bidTotals[index], max, bids[index] && myBids.has(bids[index][0]));
    });
  }

  const spread = bids.length && asks.length ? asks[0][0] - bids[0][0] : null;
  const mid = spread === null ? null : (asks[0][0] + bids[0][0]) / 2;
  $("spread").textContent = spread === null
    ? "No spread"
    : `Spread ${price(spread)} · ${((spread / mid) * 100).toFixed(2)}%`;
  $("spread-top").textContent = spread === null ? "—" : price(spread);
  const last = $("mid-last");
  last.textContent = state.last === null ? "—" : `${price(state.last)} ${arrow()}`;
  last.className = `mid-last ${tone()}`;
  const share = bidSum + askSum ? Math.round((100 * bidSum) / (bidSum + askSum)) : 50;
  $("bid-share").textContent = `B ${share}%`;
  $("ask-share").textContent = `${100 - share}% S`;
  $("bid-bar").style.width = `${share}%`;
  if (!$("price").value && mid !== null) $("price").value = price(Math.round(mid));
}

const arrow = () => (state.direction > 0 ? "↑" : state.direction < 0 ? "↓" : "");
const tone = () => (state.direction > 0 ? "up" : state.direction < 0 ? "down" : "");

// ---------- The book, order by order ----------

// The best levels as the server last showed them, each order with its id and what it
// shows, in the order they trade.
const unpack = (levels) => levels.map(([at, flat]) => {
  const orders = [];
  for (let index = 0; index < flat.length; index += 2) orders.push({ id: flat[index], qty: flat[index + 1] });
  return [at, orders];
});

const book = { mode: load("exchange-book-mode") === "orders" ? "orders" : "levels", width: 0 };

function setBookMode(mode) {
  book.mode = mode;
  book.width = 0;
  save("exchange-book-mode", mode);
  document.querySelector(".book-panel").classList.toggle("orders-mode", mode === "orders");
  choose("[data-book]", "book", mode);
  for (const slot of [...askSlots, ...bidSlots]) clearQueue(slot);
  schedule("book");
}

function clearQueue(slot) {
  slot.blocks.replaceChildren();
  slot.blockMap.clear();
  slot.queuePrice = null;
}

// One level's orders as blocks as wide as what they show, first in line on the left.
// Blocks that went shrink away where they were; new ones grow in at the back.
function showQueue(slot, level, scale, mine) {
  if (!level) {
    show(slot, null);
    clearQueue(slot);
    return;
  }
  const [at, orders] = level;
  if (slot.queuePrice !== at) clearQueue(slot);
  const fresh = slot.queuePrice === null;
  slot.queuePrice = at;
  const wanted = [];
  let total = 0;
  let yours = false;
  for (const order of orders) {
    const key = order.id || "more";
    let block = slot.blockMap.get(key);
    const added = !block;
    if (added) {
      block = document.createElement("i");
      slot.blockMap.set(key, block);
    }
    const isMine = mine.has(order.id);
    yours ||= isMine;
    block.className = `blk${order.id === 0 ? " more" : ""}${isMine ? " mine" : ""}${added && !fresh ? " new" : ""}`;
    block.style.width = `${Math.max(3, Math.round(order.qty * scale))}px`;
    block.title = order.id === 0
      ? `${size(order.qty)} ${market.base} more, behind`
      : `Order #${count(order.id)}: ${size(order.qty)} ${market.base}${isMine ? ", yours" : ""}`;
    wanted.push(block);
    total += order.qty;
  }
  const keep = new Set(wanted);
  for (const [key, block] of slot.blockMap) {
    if (keep.has(block)) continue;
    slot.blockMap.delete(key);
    block.classList.add("gone");
    setTimeout(() => block.remove(), 400);
  }
  let cursor = slot.blocks.firstChild;
  for (const block of wanted) {
    while (cursor && cursor.classList.contains("gone")) cursor = cursor.nextSibling;
    if (cursor === block) cursor = cursor.nextSibling;
    else slot.blocks.insertBefore(block, cursor);
  }
  slot.at.textContent = price(at);
  slot.at.className = yours ? "price mine" : "price";
  slot.row.className = "level";
  slot.row.title = `${orders.length} order${orders.length === 1 ? "" : "s"}, ${size(total)} ${market.base} at ${price(at)}: the first in line trades first`;
  slot.price = at;
  slot.size = total;
}

function renderQueues() {
  const mine = new Set(state.orders.keys());
  const asks = state.queues.sell.slice(0, DEPTH);
  const bids = state.queues.buy.slice(0, DEPTH);
  // One scale for every level, so that blocks compare across the book.
  const width = book.width || (book.width = askSlots[0].blocks.clientWidth || 180);
  let most = 1;
  let crowd = 1;
  for (const [, orders] of [...asks, ...bids]) {
    most = Math.max(most, orders.reduce((sum, order) => sum + order.qty, 0));
    crowd = Math.max(crowd, orders.length);
  }
  const scale = Math.max(0, width - 2 * crowd) / most;
  askSlots.forEach((slot, index) => showQueue(slot, asks[DEPTH - 1 - index], scale, mine));
  bidSlots.forEach((slot, index) => showQueue(slot, bids[index], scale, mine));
}

// Where an open order stands in its queue, if its level is among those shown.
function queueSpot(order) {
  const level = state.queues[order.side].find(([at]) => at === order.price);
  if (!level) return null;
  let ahead = 0;
  for (const [index, entry] of level[1].entries()) {
    if (entry.id === order.id) return { place: index + 1, ahead };
    ahead += entry.qty;
  }
  return null;
}

// ---------- Engine room ----------

const engine = {
  seq: 0,
  shown: 0,
  counting: false,
  paused: false,
  held: [],
  heat: [], // a second each: {buckets, p50, p99, max, cps}
};

const TIFS = { ioc: "IOC", fok: "FOK", post_only: "POST" };

function span(text, className) {
  const element = document.createElement("span");
  element.textContent = text;
  if (className) element.className = className;
  return element;
}

// What a command asked for, as the log shows it.
function commandOf(entry) {
  const parts = [];
  const sideOf = () => span(entry.side === "buy" ? "BUY" : "SELL", entry.side);
  switch (entry.cmd) {
    case "limit":
      parts.push(sideOf(), ` ${size(entry.qty)} @ ${price(entry.price)}`);
      if (TIFS[entry.tif]) parts.push(span(TIFS[entry.tif], "log-tag"));
      if (entry.display) parts.push(span(`ICEBERG ${size(entry.display)}`, "log-tag"));
      break;
    case "market":
      parts.push(sideOf(), ` ${size(entry.qty)} `, span("MARKET", "log-tag"));
      break;
    case "stop":
      parts.push(span("STOP ", "verb"), sideOf(), ` ${size(entry.qty)} @ ${price(entry.trigger)}`);
      if (entry.price !== null) parts.push(` limit ${price(entry.price)}`);
      break;
    case "cancel":
      parts.push(span("CANCEL", "verb"), ` #${count(entry.id)}`);
      break;
    case "modify":
      parts.push(span("MODIFY", "verb"), ` #${count(entry.id)} → ${size(entry.qty)} @ ${price(entry.price)}`);
      break;
    case "cancel_all":
      parts.push(span("CANCEL ALL", "verb"));
      break;
    case "set_phase":
      parts.push(span("PHASE", "verb"), ` ${entry.phase}`);
      break;
    default:
      parts.push(entry.cmd);
  }
  return parts;
}

// What came of it.
function outcomeOf(entry) {
  if (entry.rejected) return [`refused: ${entry.rejected.replaceAll("_", " ")}`, "refused"];
  const parts = [];
  if (entry.trades) parts.push(`${entry.trades} fill${entry.trades === 1 ? "" : "s"} · ${size(entry.traded)}`);
  if (entry.rested) parts.push(`rests ${size(entry.rested)}`);
  if (entry.cancelled) {
    parts.push(entry.cmd === "cancel_all"
      ? `${count(entry.cancelled)} cancelled`
      : `${size(entry.cancelled_qty)} cancelled`);
  }
  if (!parts.length) {
    parts.push({ stop: "waits for its trigger", cancel_all: "none open" }[entry.cmd] || "done");
  }
  return [parts.join(" · "), entry.trades ? `fill ${entry.side || ""}` : ""];
}

function logRow(entry) {
  const row = document.createElement("div");
  const you = entry.paper && entry.owner === state.account;
  row.className = `log-row enter${you ? " mine" : ""}`;
  // In a mirrored market, the accounts without paper money are the venue's orders and trades.
  const robot = market.source ? market.source.toUpperCase() : `BOT ${entry.owner}`;
  const who = you ? "YOU" : entry.paper ? `GUEST ${entry.owner}` : robot;
  const command = span("", "log-cmd");
  command.append(...commandOf(entry));
  const [text, kind] = outcomeOf(entry);
  row.append(
    span(`#${count(entry.seq)}`, "log-seq"),
    span(who, `log-who ${you ? "you" : entry.paper ? "guest" : "bot"}`),
    command,
    span(text, `log-out ${kind}`),
  );
  return row;
}

function appendLog(entries, skipped) {
  const log = $("log");
  if (skipped) log.prepend(span(`… ${count(skipped)} more commands in between`, "log-gap"));
  for (const entry of entries) log.prepend(logRow(entry));
  while (log.children.length > 48) log.lastElementChild.remove();
}

function engineLog(message) {
  const entries = message.entries;
  if (!entries.length) return;
  engine.seq = Math.max(engine.seq, entries[entries.length - 1].seq);
  countUp();
  if (engine.paused) {
    engine.held.push({ entries, skipped: message.skipped });
    if (engine.held.length > 10) engine.held.shift();
    return;
  }
  appendLog(entries, message.skipped);
}

// The sequence number rolls up to the latest, like an odometer.
function countUp() {
  if (engine.counting) return;
  engine.counting = true;
  const step = () => {
    const gap = engine.seq - engine.shown;
    if (engine.shown === 0 || gap > 100_000) engine.shown = engine.seq;
    else engine.shown += Math.max(1, Math.ceil(gap * 0.12));
    $("seq").textContent = count(engine.shown);
    if (engine.shown < engine.seq) requestAnimationFrame(step);
    else engine.counting = false;
  };
  requestAnimationFrame(step);
}

const logPanel = document.querySelector(".log-panel");
logPanel.addEventListener("pointerenter", () => {
  engine.paused = true;
  $("log-state").textContent = "Paused";
  $("log-state").className = "hint paused";
});
logPanel.addEventListener("pointerleave", () => {
  engine.paused = false;
  $("log-state").textContent = "Hover to pause";
  $("log-state").className = "hint";
  for (const { entries, skipped } of engine.held) appendLog(entries, skipped);
  engine.held = [];
});

function sized(canvas) {
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (!width || !height) return null;
  if (canvas.width !== Math.round(width * ratio) || canvas.height !== Math.round(height * ratio)) {
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, width, height);
  return { context, width, height };
}

// The accent colour at `alpha`.
function accent(alpha) {
  const hex = colors().accent.replace("#", "");
  const [r, g, b] = [0, 2, 4].map((at) => parseInt(hex.slice(at, at + 2), 16));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

const SECONDS = 120;
const BUCKETS = 18;
// Where a turn of `ns` falls among the buckets, continuously: 1 at a microsecond, one more
// for each doubling.
const bucketOf = (ns) => (ns < 1_000 ? ns / 1_000 : Math.min(BUCKETS, 1 + Math.log2(ns / 1_000)));

function drawHeat() {
  const area = sized($("heat"));
  if (!area) return;
  const { context, width, height } = area;
  const color = colors();
  const left = 44;
  const top = 6;
  const bottom = 18;
  const plotWidth = width - left - 4;
  const plotHeight = height - top - bottom;
  const column = plotWidth / SECONDS;
  const row = plotHeight / BUCKETS;
  const y = (bucket) => top + plotHeight - bucket * row;
  context.font = `10.5px ${color.font}`;
  context.textBaseline = "middle";
  context.textAlign = "right";
  context.fillStyle = color.muted;
  const labels = [[1, "1µs"], [3, "4µs"], [5, "16µs"], [7, "64µs"], [9, "256µs"], [11, "1ms"], [13, "4ms"], [15, "16ms"], [17, "65ms"]];
  for (const [bucket, label] of labels) {
    context.fillText(label, left - 6, y(bucket));
    context.strokeStyle = color.line;
    context.globalAlpha = 0.5;
    context.beginPath();
    context.moveTo(left, Math.round(y(bucket)) + 0.5);
    context.lineTo(left + plotWidth, Math.round(y(bucket)) + 0.5);
    context.stroke();
    context.globalAlpha = 1;
  }
  const seconds = engine.heat;
  let most = 1;
  for (const second of seconds) for (const n of second.buckets) most = Math.max(most, n);
  const x = (index) => left + plotWidth - (seconds.length - index) * column;
  seconds.forEach((second, index) => {
    second.buckets.forEach((n, bucket) => {
      if (!n) return;
      context.fillStyle = accent(0.18 + 0.82 * Math.sqrt(n / most));
      context.fillRect(x(index) + 0.5, y(bucket + 1) + 0.5, Math.max(1, column - 1), Math.max(1, row - 1));
    });
  });
  // The percentiles, as lines through the columns.
  for (const [key, stroke, alpha] of [["p50", color.text, 0.85], ["p99", color.sell, 0.9]]) {
    context.strokeStyle = stroke;
    context.globalAlpha = alpha;
    context.lineWidth = 1.5;
    context.beginPath();
    let drawing = false;
    seconds.forEach((second, index) => {
      if (!second[key]) {
        drawing = false;
        return;
      }
      const at = [x(index) + column / 2, y(bucketOf(second[key]))];
      if (drawing) context.lineTo(...at);
      else context.moveTo(...at);
      drawing = true;
    });
    context.stroke();
  }
  context.globalAlpha = 1;
  context.lineWidth = 1;
  context.textAlign = "center";
  context.fillStyle = color.muted;
  context.fillText("2 min ago", left + 26, height - bottom / 2 + 2);
  context.fillText("1 min", left + plotWidth / 2, height - bottom / 2 + 2);
  context.textAlign = "right";
  context.fillText("now", left + plotWidth, height - bottom / 2 + 2);
}

function drawTps() {
  const area = sized($("tps"));
  if (!area) return;
  const { context, width, height } = area;
  const color = colors();
  const values = engine.heat.map((second) => second.cps);
  if (values.length < 2) return;
  const most = Math.max(10, ...values) * 1.15;
  const top = 4;
  const bottom = 4;
  const x = (index) => width - (values.length - 1 - index) * (width / (SECONDS - 1));
  const y = (value) => top + (1 - value / most) * (height - top - bottom);
  const gradient = context.createLinearGradient(0, top, 0, height);
  gradient.addColorStop(0, accent(0.35));
  gradient.addColorStop(1, accent(0));
  context.beginPath();
  values.forEach((value, index) => (index ? context.lineTo(x(index), y(value)) : context.moveTo(x(index), y(value))));
  context.strokeStyle = color.accent;
  context.lineWidth = 1.75;
  context.stroke();
  context.lineTo(x(values.length - 1), height);
  context.lineTo(x(0), height);
  context.closePath();
  context.fillStyle = gradient;
  context.fill();
  const peak = Math.max(...values);
  context.font = `10.5px ${color.font}`;
  context.fillStyle = color.muted;
  context.textAlign = "left";
  context.textBaseline = "top";
  context.fillText(`peak ${count(peak)}/s`, 4, 4);
}

function renderLeaders() {
  const data = state.leaders;
  const leaders = data ? data.leaders : [];
  $("leaders-empty").hidden = leaders.length > 0;
  $("leaders").replaceChildren(
    ...leaders.map((leader, index) => {
      const row = document.createElement("tr");
      const me = leader.account === state.account;
      if (me) row.className = "me";
      const rank = span(String(index + 1), index < 3 ? "rank top" : "rank");
      const name = span(me ? "You" : `Guest #${leader.account}`, me ? "trader me" : "trader");
      const profit = `${leader.profit >= 0 ? "+" : "−"}${money(Math.abs(leader.profit))}`;
      row.append(
        cell(rank, "left"),
        cell(name, "left"),
        cell(profit, leader.profit > 0 ? "up" : leader.profit < 0 ? "down" : ""),
        cell(money(leader.value), "muted"),
      );
      return row;
    }),
  );
  const mine = state.rank;
  const line = $("my-rank");
  if (mine) {
    line.replaceChildren("You are ", bold(`#${mine.rank}`), ` of ${count(mine.of)} traders`);
  } else line.textContent = "Make a trade to join the leaderboard.";
}

// ---------- Trades ----------

function renderTrades() {
  $("trades").replaceChildren(
    ...state.trades.slice(0, 40).map((trade) => {
      const row = document.createElement("div");
      row.className = trade.shown ? `trade-row ${trade.side}` : `trade-row ${trade.side} fresh`;
      trade.shown = true;
      for (const text of [price(trade.price), size(trade.qty), timeOf(trade.time)]) {
        const cell = document.createElement("span");
        cell.textContent = text;
        row.append(cell);
      }
      return row;
    }),
  );
}

// ---------- Ticker ----------

function renderTicker() {
  const last = $("last");
  last.textContent = state.last === null ? "—" : price(state.last);
  last.className = `last ${tone()}`;
  $("last-direction").textContent = arrow();
  $("last-direction").className = `direction ${tone()}`;
  const candles = chart.base;
  if (!candles.length) return;
  let high = -Infinity;
  let low = Infinity;
  let volume = 0;
  for (const candle of candles) {
    high = Math.max(high, candle.h);
    low = Math.min(low, candle.l);
    volume += candle.v;
  }
  const open = candles[0].o;
  const close = state.last ?? candles[candles.length - 1].c;
  const change = ((close - open) / open) * 100;
  const element = $("change");
  element.textContent = `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`;
  element.className = change >= 0 ? "up" : "down";
  $("high").textContent = price(high);
  $("low").textContent = price(low);
  $("volume").textContent = `${size(volume)} ${market.base}`;
}

// ---------- Chart ----------

const chart = {
  base: [], // the server's candles, then the trades since: {t, o, h, l, c, v}
  view: "price",
  interval: INTERVAL,
  style: "candles",
  hover: null,
  colors: null,
};

// A trade, into the candle of the server's clock.
function record(at, qty) {
  const second = Math.floor(now() / 1_000);
  const start = second - (second % INTERVAL);
  const base = chart.base;
  const last = base[base.length - 1];
  if (last && last.t >= start) {
    last.h = Math.max(last.h, at);
    last.l = Math.min(last.l, at);
    last.c = at;
    last.v += qty;
  } else {
    base.push({ t: start, o: at, h: at, l: at, c: at, v: qty });
  }
  while (base.length && base[0].t <= start - KEPT) base.shift();
}

// The candles at the chosen length.
function series() {
  const size = chart.interval;
  if (size === INTERVAL) return chart.base;
  const out = [];
  for (const candle of chart.base) {
    const t = candle.t - (candle.t % size);
    const last = out[out.length - 1];
    if (last && last.t === t) {
      last.h = Math.max(last.h, candle.h);
      last.l = Math.min(last.l, candle.l);
      last.c = candle.c;
      last.v += candle.v;
    } else {
      out.push({ ...candle, t });
    }
  }
  return out;
}

// A moving average of closes, by candle time.
function average(candles, length) {
  const out = new Map();
  let sum = 0;
  candles.forEach((candle, index) => {
    sum += candle.c;
    if (index >= length) sum -= candles[index - length].c;
    if (index >= length - 1) out.set(candle.t, sum / length);
  });
  return out;
}

// A step of 1, 2 or 5 times a power of ten ticks, about `rough` long.
function niceStep(rough) {
  const power = 10 ** Math.floor(Math.log10(Math.max(rough, 1)));
  for (const factor of [1, 2, 5, 10]) {
    if (factor * power >= rough) return Math.max(1, factor * power);
  }
  return 10 * power;
}

function colors() {
  if (!chart.colors) {
    const style = getComputedStyle(document.documentElement);
    const take = (name) => style.getPropertyValue(name).trim();
    chart.colors = {
      buy: take("--buy"),
      sell: take("--sell"),
      buySoft: take("--buy-soft"),
      sellSoft: take("--sell-soft"),
      line: take("--line"),
      muted: take("--muted"),
      text: take("--text"),
      accent: take("--accent"),
      panel: take("--panel"),
      bg: take("--bg"),
      font: take("--font"),
    };
  }
  return chart.colors;
}

function box(context, x, y, width, height, fill) {
  context.fillStyle = fill;
  context.beginPath();
  if (context.roundRect) context.roundRect(x, y, width, height, 3);
  else context.rect(x, y, width, height);
  context.fill();
}

function drawChart() {
  const canvas = $("canvas");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (!width || !height) return;
  if (canvas.width !== Math.round(width * ratio) || canvas.height !== Math.round(height * ratio)) {
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, width, height);
  if (chart.view === "depth") {
    drawDepth(context, width, height);
    return;
  }
  const candles = series();
  $("chart-empty").hidden = candles.length > 0;
  if (!candles.length) {
    $("legend").replaceChildren();
    return;
  }
  const color = colors();
  const axis = 64;
  const top = 26;
  const bottom = 22;
  const plotWidth = width - axis;
  const plotHeight = height - top - bottom;
  const volumeHeight = Math.round(plotHeight * 0.16);
  const priceHeight = plotHeight - volumeHeight - 8;
  const size = chart.interval;
  // About a hundred candles; wider ones while there are few.
  const wanted = Math.max(40, Math.min(110, candles.length + 6));
  const spacing = Math.max(5, Math.min(20, plotWidth / wanted));
  const slots = Math.floor(plotWidth / spacing);
  const second = Math.floor(now() / 1_000);
  const end = Math.max(second - (second % size), candles[candles.length - 1].t);
  const start = end - (slots - 1) * size;
  const x = (t) => plotWidth - spacing / 2 - ((end - t) / size) * spacing;
  const visible = candles.filter((candle) => candle.t >= start);
  const ma = average(candles, 20);

  let low = Infinity;
  let high = -Infinity;
  let maxVolume = 0;
  for (const candle of visible) {
    low = Math.min(low, candle.l);
    high = Math.max(high, candle.h);
    maxVolume = Math.max(maxVolume, candle.v);
    const mean = ma.get(candle.t);
    if (mean !== undefined) {
      low = Math.min(low, mean);
      high = Math.max(high, mean);
    }
  }
  const lastPrice = state.last ?? candles[candles.length - 1].c;
  low = Math.min(low, lastPrice);
  high = Math.max(high, lastPrice);
  const margin = Math.max((high - low) * 0.08, 2);
  low -= margin;
  high += margin;
  const y = (at) => top + (1 - (at - low) / (high - low)) * priceHeight;

  context.font = `11px ${color.font}`;
  context.textBaseline = "middle";
  context.lineWidth = 1;

  // The price grid, and its labels on the right, but for those the last price covers.
  const step = niceStep((high - low) / 6);
  const lastY = y(lastPrice);
  context.textAlign = "left";
  for (let at = Math.ceil(low / step) * step; at <= high; at += step) {
    const row = Math.round(y(at)) + 0.5;
    context.strokeStyle = color.line;
    context.beginPath();
    context.moveTo(0, row);
    context.lineTo(plotWidth, row);
    context.stroke();
    if (Math.abs(row - lastY) < 14) continue;
    context.fillStyle = color.muted;
    context.fillText(price(at), plotWidth + 8, row);
  }

  // The time grid, a label at least 90 pixels apart.
  const labelEvery = [1, 2, 3, 4, 6, 12, 24, 36, 72]
    .map((n) => n * size)
    .find((seconds) => (seconds / size) * spacing >= 90) || 72 * size;
  context.textAlign = "center";
  for (let t = Math.ceil(start / labelEvery) * labelEvery; t <= end; t += labelEvery) {
    const column = Math.round(x(t)) + 0.5;
    context.strokeStyle = color.line;
    context.globalAlpha = 0.55;
    context.beginPath();
    context.moveTo(column, top);
    context.lineTo(column, top + plotHeight);
    context.stroke();
    context.globalAlpha = 1;
    const label = labelEvery % 60 === 0 ? timeOf(t * 1_000).slice(0, 5) : timeOf(t * 1_000);
    const half = context.measureText(label).width / 2;
    if (column - half < 0 || column + half > plotWidth) continue;
    context.fillStyle = color.muted;
    context.fillText(label, column, height - bottom / 2);
  }

  // Volume along the bottom.
  // An odd width, so a candle's body is centred on its wick.
  const body = Math.max(1, Math.floor(spacing * 0.66)) | 1;
  const left = (t) => Math.round(x(t)) - (body - 1) / 2;
  for (const candle of visible) {
    const tall = maxVolume ? Math.max(1, (candle.v / maxVolume) * volumeHeight) : 0;
    context.fillStyle = candle.c >= candle.o ? color.buySoft : color.sellSoft;
    context.fillRect(left(candle.t), top + plotHeight - tall, body, tall);
  }

  // The candles, or a line through the closes.
  if (chart.style === "candles") {
    for (const candle of visible) {
      const rising = candle.c >= candle.o;
      const column = Math.round(x(candle.t));
      context.strokeStyle = context.fillStyle = rising ? color.buy : color.sell;
      context.beginPath();
      context.moveTo(column + 0.5, Math.round(y(candle.h)));
      context.lineTo(column + 0.5, Math.round(y(candle.l)));
      context.stroke();
      const from = Math.round(y(Math.max(candle.o, candle.c)));
      const to = Math.round(y(Math.min(candle.o, candle.c)));
      context.fillRect(left(candle.t), from, body, Math.max(1, to - from));
    }
  } else if (visible.length) {
    const rising = lastPrice >= visible[0].o;
    const stroke = rising ? color.buy : color.sell;
    const gradient = context.createLinearGradient(0, top, 0, top + priceHeight);
    gradient.addColorStop(0, rising ? color.buySoft : color.sellSoft);
    gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
    context.beginPath();
    visible.forEach((candle, index) => {
      if (index === 0) context.moveTo(x(candle.t), y(candle.c));
      else context.lineTo(x(candle.t), y(candle.c));
    });
    context.strokeStyle = stroke;
    context.lineWidth = 1.75;
    context.stroke();
    context.lineTo(x(visible[visible.length - 1].t), top + priceHeight);
    context.lineTo(x(visible[0].t), top + priceHeight);
    context.closePath();
    context.fillStyle = gradient;
    context.fill();
    context.lineWidth = 1;
  }

  // The moving average.
  context.strokeStyle = color.accent;
  context.lineWidth = 1.25;
  context.globalAlpha = 0.85;
  context.beginPath();
  let drawing = false;
  for (const candle of visible) {
    const mean = ma.get(candle.t);
    if (mean === undefined) continue;
    if (drawing) context.lineTo(x(candle.t), y(mean));
    else context.moveTo(x(candle.t), y(mean));
    drawing = true;
  }
  context.stroke();
  context.globalAlpha = 1;
  context.lineWidth = 1;

  // Your open orders, as lines at their prices.
  context.font = `600 10.5px ${color.font}`;
  for (const order of state.orders.values()) {
    if (order.price < low || order.price > high) continue;
    const row = Math.round(y(order.price)) + 0.5;
    context.strokeStyle = color.accent;
    context.setLineDash([6, 4]);
    context.beginPath();
    context.moveTo(0, row);
    context.lineTo(plotWidth, row);
    context.stroke();
    context.setLineDash([]);
    const label = `${order.side === "buy" ? "BUY" : "SELL"} ${size(order.leaves)} @ ${price(order.price)}`;
    const labelWidth = context.measureText(label).width + 12;
    box(context, 6, row - 9, labelWidth, 18, color.accent);
    context.fillStyle = color.bg;
    context.textAlign = "left";
    context.fillText(label, 12, row);
  }
  context.font = `11px ${color.font}`;
  // Your fills, as markers on their candles: below for a buy, above for a sell.
  for (const fill of state.fills) {
    const second = Math.floor(fill.time / 1_000);
    const t = second - (second % size);
    if (t < start || fill.price < low || fill.price > high) continue;
    const cx = Math.round(x(t)) + 0.5;
    const cy = y(fill.price);
    const tip = fill.side === "buy" ? 1 : -1;
    context.beginPath();
    context.moveTo(cx, cy + 3 * tip);
    context.lineTo(cx - 5, cy + 11 * tip);
    context.lineTo(cx + 5, cy + 11 * tip);
    context.closePath();
    context.fillStyle = fill.side === "buy" ? color.buy : color.sell;
    context.strokeStyle = color.text;
    context.fill();
    context.stroke();
  }

  // The last price, across the chart and on the axis.
  const lastRow = Math.round(y(lastPrice)) + 0.5;
  const lastColor = state.direction < 0 ? color.sell : color.buy;
  context.strokeStyle = lastColor;
  context.setLineDash([3, 3]);
  context.beginPath();
  context.moveTo(0, lastRow);
  context.lineTo(plotWidth, lastRow);
  context.stroke();
  context.setLineDash([]);
  box(context, plotWidth + 2, lastRow - 9, axis - 4, 18, lastColor);
  context.fillStyle = "#fff";
  context.textAlign = "left";
  context.fillText(price(lastPrice), plotWidth + 8, lastRow);

  // The crosshair.
  let shown = candles[candles.length - 1];
  const hover = chart.hover;
  if (hover && hover.x >= 0 && hover.x < plotWidth && hover.y >= top && hover.y <= top + plotHeight) {
    const back = Math.round((plotWidth - spacing / 2 - hover.x) / spacing);
    const t = end - back * size;
    const column = Math.round(x(t)) + 0.5;
    const row = Math.round(hover.y) + 0.5;
    context.strokeStyle = color.muted;
    context.setLineDash([4, 4]);
    context.beginPath();
    context.moveTo(column, top);
    context.lineTo(column, top + plotHeight);
    context.moveTo(0, row);
    context.lineTo(plotWidth, row);
    context.stroke();
    context.setLineDash([]);
    if (hover.y <= top + priceHeight) {
      const at = low + (1 - (hover.y - top) / priceHeight) * (high - low);
      box(context, plotWidth + 2, row - 9, axis - 4, 18, color.line);
      context.fillStyle = color.text;
      context.textAlign = "left";
      context.fillText(price(Math.round(at)), plotWidth + 8, row);
    }
    const label = timeOf(t * 1_000);
    const labelWidth = context.measureText(label).width + 12;
    box(context, column - labelWidth / 2, height - bottom + 2, labelWidth, bottom - 4, color.line);
    context.fillStyle = color.text;
    context.textAlign = "center";
    context.fillText(label, column, height - bottom / 2);
    shown = candles.find((candle) => candle.t === t) || null;
  }
  legend(shown, ma);
}

// The book's cumulative size on each side of the mid, out to its deepest level.
function drawDepth(context, width, height) {
  const color = colors();
  const bids = sorted("buy");
  const asks = sorted("sell");
  $("chart-empty").hidden = bids.length > 0 && asks.length > 0;
  if (!bids.length || !asks.length) {
    $("legend").replaceChildren();
    return;
  }
  const axis = 64;
  const top = 26;
  const bottom = 22;
  const plotWidth = width - axis;
  const plotHeight = height - top - bottom;
  const mid = (bids[0][0] + asks[0][0]) / 2;
  const half = Math.max(mid - bids[bids.length - 1][0], asks[asks.length - 1][0] - mid, 1) * 1.04;
  const low = mid - half;
  const high = mid + half;
  const totals = (levels) => {
    let sum = 0;
    return levels.map(([at, level]) => [at, (sum += level.qty)]);
  };
  const bidSteps = totals(bids);
  const askSteps = totals(asks);
  const most = Math.max(bidSteps[bidSteps.length - 1][1], askSteps[askSteps.length - 1][1]) * 1.15;
  const x = (at) => ((at - low) / (high - low)) * plotWidth;
  const y = (qty) => top + (1 - qty / most) * plotHeight;

  context.font = `11px ${color.font}`;
  context.textBaseline = "middle";
  context.lineWidth = 1;
  // The size grid on the right, and prices along the bottom.
  const step = niceStep(most / 5);
  context.textAlign = "left";
  for (let qty = step; qty < most; qty += step) {
    const row = Math.round(y(qty)) + 0.5;
    context.strokeStyle = color.line;
    context.beginPath();
    context.moveTo(0, row);
    context.lineTo(plotWidth, row);
    context.stroke();
    context.fillStyle = color.muted;
    context.fillText(size(qty), plotWidth + 8, row);
  }
  const priceStep = niceStep((high - low) / 6);
  context.textAlign = "center";
  for (let at = Math.ceil(low / priceStep) * priceStep; at <= high; at += priceStep) {
    const column = x(at);
    if (column < 20 || column > plotWidth - 20) continue;
    context.fillStyle = color.muted;
    context.fillText(price(at), column, height - bottom / 2);
  }

  // The spread, between the two sides.
  context.fillStyle = color.hover || "rgba(230, 237, 243, 0.05)";
  context.fillRect(x(bids[0][0]), top, x(asks[0][0]) - x(bids[0][0]), plotHeight);

  // Each side as steps: flat across a level's price, up at the next one.
  const side = (steps, edge, stroke, fill) => {
    const path = new Path2D();
    path.moveTo(x(steps[0][0]), y(0));
    steps.forEach(([at, total], index) => {
      if (index > 0) path.lineTo(x(at), y(steps[index - 1][1]));
      path.lineTo(x(at), y(total));
    });
    path.lineTo(edge, y(steps[steps.length - 1][1]));
    const area = new Path2D(path);
    area.lineTo(edge, y(0));
    area.closePath();
    const gradient = context.createLinearGradient(0, top, 0, top + plotHeight);
    gradient.addColorStop(0, fill);
    gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
    context.fillStyle = gradient;
    context.fill(area);
    context.strokeStyle = stroke;
    context.lineWidth = 1.75;
    context.stroke(path);
    context.lineWidth = 1;
  };
  side(bidSteps, 0, color.buy, color.buySoft);
  side(askSteps, plotWidth, color.sell, color.sellSoft);

  // The mid.
  const midColumn = Math.round(x(mid)) + 0.5;
  context.strokeStyle = color.muted;
  context.setLineDash([3, 4]);
  context.beginPath();
  context.moveTo(midColumn, top);
  context.lineTo(midColumn, top + plotHeight);
  context.stroke();
  context.setLineDash([]);

  // Your orders, as dots on their side's curve.
  const totalAt = (steps, at, buying) => {
    let total = 0;
    for (const [level, sum] of steps) if (buying ? level >= at : level <= at) total = sum;
    return total;
  };
  for (const order of state.orders.values()) {
    if (order.price < low || order.price > high) continue;
    const buying = order.side === "buy";
    const cy = y(totalAt(buying ? bidSteps : askSteps, order.price, buying));
    context.beginPath();
    context.arc(x(order.price), cy, 4.5, 0, 2 * Math.PI);
    context.fillStyle = color.accent;
    context.fill();
    context.strokeStyle = color.bg;
    context.lineWidth = 2;
    context.stroke();
    context.lineWidth = 1;
  }

  // The crosshair: what it would take to reach a price.
  const hover = chart.hover;
  let shownAt = null;
  if (hover && hover.x >= 0 && hover.x < plotWidth && hover.y >= top && hover.y <= top + plotHeight) {
    const at = Math.round(low + (hover.x / plotWidth) * (high - low));
    const buying = at <= mid;
    const steps = buying ? bidSteps : askSteps;
    const total = totalAt(steps, at, buying);
    let value = 0;
    for (const [level, levelData] of buying ? bids : asks) {
      if (buying ? level >= at : level <= at) value += level * levelData.qty;
    }
    const column = Math.round(hover.x) + 0.5;
    context.strokeStyle = color.muted;
    context.setLineDash([4, 4]);
    context.beginPath();
    context.moveTo(column, top);
    context.lineTo(column, top + plotHeight);
    context.stroke();
    context.setLineDash([]);
    if (total > 0) {
      context.beginPath();
      context.arc(column, y(total), 4, 0, 2 * Math.PI);
      context.fillStyle = buying ? color.buy : color.sell;
      context.fill();
    }
    const lines = [`${price(at)} ${market.quote}`, `${size(total)} ${market.base}`, money(value)];
    context.font = `600 11px ${color.font}`;
    const boxWidth = Math.max(...lines.map((line) => context.measureText(line).width)) + 16;
    const left = hover.x + boxWidth + 14 > plotWidth ? hover.x - boxWidth - 10 : hover.x + 10;
    const boxTop = Math.max(top, Math.min(hover.y - 30, top + plotHeight - 56));
    box(context, left, boxTop, boxWidth, 56, color.panel);
    context.strokeStyle = color.line;
    context.strokeRect(left + 0.5, boxTop + 0.5, boxWidth - 1, 55);
    context.textAlign = "left";
    lines.forEach((line, index) => {
      context.fillStyle = index === 0 ? color.text : color.muted;
      context.fillText(line, left + 8, boxTop + 12 + index * 16);
    });
    context.font = `11px ${color.font}`;
    shownAt = { at, total, buying };
  }
  const parts = [];
  const item = (label, value, className) => {
    const element = document.createElement("span");
    element.append(label);
    const b = bold(value);
    if (className) b.className = className;
    element.append(b);
    parts.push(element);
  };
  parts.push(span(`${market.symbol} · depth`));
  item("Bids", `${size(bidSteps[bidSteps.length - 1][1])} ${market.base}`, "up");
  item("Asks", `${size(askSteps[askSteps.length - 1][1])} ${market.base}`, "down");
  item("Mid", price(Math.round(mid)));
  item("Spread", price(asks[0][0] - bids[0][0]));
  if (shownAt) item(shownAt.buying ? "Sell into" : "Buy up to", price(shownAt.at));
  $("legend").replaceChildren(...parts);
}

function legend(candle, ma) {
  const parts = [];
  const item = (label, value, className) => {
    const span = document.createElement("span");
    span.append(label);
    const b = bold(value);
    if (className) b.className = className;
    span.append(b);
    parts.push(span);
  };
  const name = document.createElement("span");
  name.textContent = `${market.symbol} · ${chart.interval < 60 ? `${chart.interval}s` : "1m"}`;
  parts.push(name);
  if (candle) {
    const tone = candle.c >= candle.o ? "up" : "down";
    item("O", price(candle.o), tone);
    item("H", price(candle.h), tone);
    item("L", price(candle.l), tone);
    item("C", price(candle.c), tone);
    const change = ((candle.c - candle.o) / candle.o) * 100;
    item("", `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`, tone);
    item("V", size(candle.v));
    const mean = ma.get(candle.t);
    if (mean !== undefined) {
      const span = document.createElement("span");
      span.className = "ma";
      span.append("MA 20", bold(price(Math.round(mean))));
      parts.push(span);
    }
  }
  $("legend").replaceChildren(...parts);
}

const canvas = $("canvas");
canvas.addEventListener("pointermove", (event) => {
  const bounds = canvas.getBoundingClientRect();
  chart.hover = { x: event.clientX - bounds.left, y: event.clientY - bounds.top };
  schedule("chart");
});
canvas.addEventListener("pointerleave", () => {
  chart.hover = null;
  schedule("chart");
});
new ResizeObserver(() => schedule("chart")).observe($("canvas"));

function choose(selector, attribute, value) {
  for (const button of document.querySelectorAll(selector)) {
    const on = button.dataset[attribute] === String(value);
    button.classList.toggle("active", on);
    button.setAttribute("aria-pressed", String(on));
  }
}
for (const button of document.querySelectorAll("[data-interval]")) {
  button.addEventListener("click", () => {
    chart.interval = Number(button.dataset.interval);
    choose("[data-interval]", "interval", chart.interval);
    schedule("chart");
  });
}
for (const button of document.querySelectorAll("[data-style]")) {
  button.addEventListener("click", () => {
    chart.style = button.dataset.style;
    choose("[data-style]", "style", chart.style);
    schedule("chart");
  });
}

// ---------- Your account ----------

function renderWallet() {
  const wallet = state.wallet;
  if (!wallet) return;
  $("cash").textContent = money(wallet.cash - wallet.cash_held);
  $("cash-held").textContent = money(wallet.cash_held);
  $("position").textContent = `${size(wallet.position - wallet.position_held)} ${market.base}`;
  $("position-held").textContent = `${size(wallet.position_held)} ${market.base}`;
  const mark = state.last ?? midPrice();
  if (mark === null) return;
  const shares = wallet.position * mark;
  const value = wallet.cash + shares;
  $("value").textContent = money(value);
  const share = value > 0 ? Math.max(0, Math.min(100, (100 * wallet.cash) / value)) : 100;
  $("alloc-cash").style.width = `${share}%`;
  const base = state.baseline;
  if (base) {
    const profit = value - (base.cash + base.position * mark);
    const start = base.cash + base.position * mark;
    const percent = start > 0 ? (profit / start) * 100 : 0;
    const element = $("pnl");
    element.textContent = `${profit >= 0 ? "+" : "−"}${money(Math.abs(profit))} (${percent >= 0 ? "+" : "−"}${Math.abs(percent).toFixed(2)}%) since you started`;
    element.className = profit > 0 ? "pnl up" : profit < 0 ? "pnl down" : "pnl";
  }
}

function midPrice() {
  const bid = sorted("buy")[0];
  const ask = sorted("sell")[0];
  return bid && ask ? Math.round((bid[0] + ask[0]) / 2) : null;
}

// ---------- Your orders ----------

let ordersTab = "open";

function cell(text, className) {
  const element = document.createElement("td");
  if (text instanceof Node) element.append(text);
  else element.textContent = text;
  if (className) element.className = className;
  return element;
}

function sideCell(side) {
  const span = document.createElement("span");
  span.className = side;
  span.textContent = sideName(side);
  return cell(span, "left");
}

function renderOrders() {
  const orders = [...state.orders.values()].sort((a, b) => b.id - a.id);
  $("open-count").textContent = orders.length;
  $("fill-count").textContent = state.fills.length;
  $("cancel-all").hidden = ordersTab !== "open" || orders.length === 0;
  $("open-empty").hidden = orders.length > 0;
  $("fills-empty").hidden = state.fills.length > 0;
  $("open-orders").replaceChildren(
    ...orders.map((order) => {
      const row = document.createElement("tr");
      let progress = "—";
      if (order.qty) {
        const done = order.qty - order.leaves;
        const bar = document.createElement("span");
        bar.className = "progress";
        const fill = document.createElement("i");
        fill.style.width = `${(100 * done) / order.qty}%`;
        bar.append(fill);
        progress = document.createDocumentFragment();
        progress.append(bar, `${Math.round((100 * done) / order.qty)}%`);
      }
      const cancel = document.createElement("button");
      cancel.className = "cancel";
      cancel.type = "button";
      cancel.title = "Cancel this order";
      cancel.setAttribute("aria-label", `Cancel order ${order.id}`);
      cancel.innerHTML = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8"/></svg>';
      cancel.addEventListener("click", () => send({ type: "cancel", id: order.id }));
      row.append(
        cell(order.time ? timeOf(order.time) : "—", "left muted"),
        sideCell(order.side),
        cell(price(order.price)),
        cell(order.qty ? `${size(order.leaves)} / ${size(order.qty)}` : size(order.leaves)),
        cell(progress),
        cell(queueCell(order)),
        cell(`#${order.id}`, "muted"),
        cell(cancel),
      );
      return row;
    }),
  );
  $("fills").replaceChildren(
    ...state.fills.map((fill) => {
      const row = document.createElement("tr");
      row.append(
        cell(timeOf(fill.time), "left muted"),
        sideCell(fill.side),
        cell(price(fill.price)),
        cell(size(fill.qty)),
        cell(money(fill.price * fill.qty)),
      );
      return row;
    }),
  );
}

// An open order's place in its queue: next in line, or how much is ahead of it.
function queueCell(order) {
  const spot = queueSpot(order);
  if (!spot) {
    const element = span("—", "queue-spot");
    element.title = "Deeper than the levels shown";
    return element;
  }
  if (spot.place === 1) return span("Next in line", "next-badge");
  const element = span("", "queue-spot");
  element.append(bold(`#${spot.place}`), ` · ${size(spot.ahead)} ahead`);
  element.title = `${spot.place - 1} order${spot.place === 2 ? "" : "s"} with ${size(spot.ahead)} ${market.base} trade before yours`;
  return element;
}

function showTab(tab) {
  ordersTab = tab;
  $("tab-open").classList.toggle("active", tab === "open");
  $("tab-fills").classList.toggle("active", tab === "fills");
  $("tab-open").setAttribute("aria-selected", String(tab === "open"));
  $("tab-fills").setAttribute("aria-selected", String(tab === "fills"));
  $("open-view").hidden = tab !== "open";
  $("fills-view").hidden = tab !== "fills";
  renderOrders();
}
$("tab-open").addEventListener("click", () => showTab("open"));
$("tab-fills").addEventListener("click", () => showTab("fills"));
$("cancel-all").addEventListener("click", () => send({ type: "cancel_all" }));

// ---------- Order ticket ----------

// What `qty` would take from the other side's levels, best first: the worst price it
// reaches, what it costs, and how much is there.
function sweep(side, qty) {
  const levels = sorted(side === "buy" ? "sell" : "buy");
  let left = qty;
  let cost = 0;
  let limit = null;
  for (const [at, level] of levels) {
    if (left <= 0) break;
    const take = Math.min(left, level.qty);
    cost += take * at;
    left -= take;
    limit = at;
  }
  return limit === null ? null : { limit, cost, filled: qty - left };
}

function free() {
  const wallet = state.wallet;
  if (!wallet) return null;
  return state.side === "buy" ? wallet.cash - wallet.cash_held : wallet.position - wallet.position_held;
}

function notice(text, error = false) {
  $("notice").textContent = text;
  $("notice").className = error ? "notice error" : "notice";
}

function updateTicket() {
  const available = free();
  $("available").textContent = available === null
    ? "—"
    : state.side === "buy" ? money(available) : `${size(available)} ${market.base}`;
  const qty = lotsOf($("qty").value);
  const buying = state.side === "buy";
  let value = null;
  let hold = null;
  if (state.type === "market") {
    const taken = qty > 0 ? sweep(state.side, qty) : null;
    $("total-label").textContent = buying ? "Estimated cost" : "Estimated proceeds";
    if (taken) {
      value = taken.cost;
      hold = buying ? taken.limit * qty : qty;
      $("total").textContent = `≈ ${money(value)}`;
    } else $("total").textContent = "—";
  } else {
    const ticks = ticksOf($("price").value);
    $("total-label").textContent = "Order value";
    if (ticks > 0 && qty > 0) {
      value = ticks * qty;
      hold = buying ? value : qty;
      $("total").textContent = money(value);
    } else $("total").textContent = "—";
  }
  const button = $("submit");
  const short = available !== null && hold !== null && hold > available;
  button.className = `submit ${state.side}`;
  button.disabled = !state.connected || short;
  if (!state.connected) button.textContent = "Connecting…";
  else if (short) button.textContent = buying ? "Not enough cash" : `Not enough ${market.base}`;
  else button.textContent = `${sideName(state.side)} ${market.base}`;
}

function setSide(side) {
  state.side = side;
  $("buy-side").classList.toggle("active", side === "buy");
  $("sell-side").classList.toggle("active", side === "sell");
  $("buy-side").setAttribute("aria-pressed", String(side === "buy"));
  $("sell-side").setAttribute("aria-pressed", String(side === "sell"));
  notice("");
  updateTicket();
}

function setType(type) {
  state.type = type;
  $("type-limit").classList.toggle("active", type === "limit");
  $("type-market").classList.toggle("active", type === "market");
  $("type-limit").setAttribute("aria-pressed", String(type === "limit"));
  $("type-market").setAttribute("aria-pressed", String(type === "market"));
  $("price-field").hidden = type === "market";
  $("tif-field").hidden = type === "market";
  $("market-field").hidden = type !== "market";
  notice("");
  updateTicket();
}

// A level clicked in the book: a limit order that would trade with it.
function pick(at, side) {
  setType("limit");
  $("price").value = price(at);
  setSide(side);
  const field = $("price-field");
  field.classList.remove("flash");
  void field.offsetWidth;
  field.classList.add("flash");
}

function useShare(share) {
  const available = free();
  if (available === null) return;
  let qty;
  if (state.side === "sell") qty = Math.floor(available * share);
  else {
    const budget = available * share;
    let at = state.type === "market"
      ? (sweep("buy", 1) || {}).limit
      : ticksOf($("price").value);
    if (!(at > 0)) {
      notice("Enter a price first", true);
      return;
    }
    qty = Math.floor(budget / at);
    // A market buy holds its worst price for all of it.
    for (let round = 0; state.type === "market" && round < 4 && qty > 0; round += 1) {
      const taken = sweep("buy", qty);
      if (!taken || taken.limit * qty <= budget) break;
      at = taken.limit;
      qty = Math.floor(budget / at);
    }
  }
  $("qty").value = plain(Math.max(0, qty));
  notice("");
  updateTicket();
}

function place(ticks, qty, tif) {
  const ref = ++state.nextRef;
  state.requests.set(ref, { side: state.side, price: ticks, qty, tif, time: now() });
  send({ type: "order", ref, side: state.side, qty, price: ticks, tif });
  notice("");
}

function submit() {
  const qty = lotsOf($("qty").value);
  if (!(qty > 0)) {
    notice("Enter an amount", true);
    return;
  }
  if (state.type === "market") {
    // A limit order at the worst price it needs, immediate or cancel: paper accounts hold
    // what an order can cost, so there are no market orders.
    const taken = sweep(state.side, qty);
    if (!taken) {
      notice("Nobody is quoting that side right now", true);
      return;
    }
    place(taken.limit, qty, "ioc");
    return;
  }
  const ticks = ticksOf($("price").value);
  if (!(ticks > 0)) {
    notice("Enter a price", true);
    return;
  }
  place(ticks, qty, $("tif").value);
}

$("buy-side").addEventListener("click", () => setSide("buy"));
$("sell-side").addEventListener("click", () => setSide("sell"));
$("type-limit").addEventListener("click", () => setType("limit"));
$("type-market").addEventListener("click", () => setType("market"));
$("price").addEventListener("input", () => schedule("ticket"));
$("qty").addEventListener("input", () => schedule("ticket"));
$("submit").addEventListener("click", submit);
for (const input of [$("price"), $("qty")]) {
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") submit();
  });
}
for (const button of document.querySelectorAll("[data-share]")) {
  button.addEventListener("click", () => useShare(Number(button.dataset.share)));
}

// ---------- Exchange statistics ----------

// Where the time went inside the engine in the last second.
function stages(message) {
  if (!message.batch_commands) return;
  const apply = message.apply_ns * message.batch_commands;
  const total = Math.max(1, message.write_ns + message.sync_ns + apply);
  $("bar-write").style.width = `${(100 * message.write_ns) / total}%`;
  $("bar-sync").style.width = `${(100 * message.sync_ns) / total}%`;
  $("bar-apply").style.width = `${(100 * apply) / total}%`;
  $("st-write").textContent = duration(message.write_ns);
  $("st-sync").textContent = duration(message.sync_ns);
  $("st-apply").textContent = duration(apply);
  $("st-batch").textContent = `(${count(message.batch_commands)} command${message.batch_commands === 1 ? "" : "s"} a turn)`;
}

// What is traded: names, decimals, and where its orders come from.
function setMarket(message) {
  const changed = message.lot_decimals !== market.lot_decimals || message.symbol !== market.symbol;
  for (const key of Object.keys(market)) market[key] = message[key] ?? null;
  for (const element of document.querySelectorAll(".m-base")) element.textContent = market.base;
  for (const element of document.querySelectorAll(".m-quote")) element.textContent = market.quote;
  $("symbol-mark").textContent = market.base.charAt(0);
  document.title = `${market.symbol} · Matching Engine · Live Exchange Demo`;
  const sub = $("symbol-sub");
  if (market.source) {
    const link = document.createElement("a");
    link.href = market.source_url || "#";
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = market.source;
    sub.replaceChildren("Real orders and trades, mirrored live from ", link, " · paper money");
    $("about-lede").textContent = `Every order you place here goes through a working exchange: it is numbered, `
      + `written to a journal, matched, and settled in your paper account. The other orders on the book are `
      + `real: the ${market.symbol} orders resting on ${market.source}, placed here as they are there, `
      + `and its trades sent again as they happen. Nothing on this page involves real funds.`;
  } else sub.textContent = "Paper market · traded by bots and you";
  if (changed) $("qty").value = market.lot_decimals > 0 ? "0.1" : "10";
  schedule("book", "trades", "chart", "ticker", "wallet", "orders", "ticket", "leaders");
}

// What the server recovered when it started.
function renderStarted() {
  const started = state.started;
  if (!started) return;
  $("started").hidden = false;
  if (started.recovered === 0) {
    $("rec-what").textContent = "Started on an empty journal";
    $("rec-from").textContent = "Nothing to recover";
  } else {
    $("rec-what").replaceChildren(
      "Recovered ", bold(count(started.recovered)), " commands in ", bold(`${count(started.recovery_ms)} ms`),
    );
    const replayed = started.recovered - started.snapshot;
    $("rec-from").replaceChildren(
      ...(started.snapshot
        ? ["Snapshot at ", bold(`#${count(started.snapshot)}`), " + ", bold(count(replayed)), " replayed"]
        : [bold(count(replayed)), " replayed from the journal"]),
      " · ", bold(count(started.orders)), " orders back",
    );
  }
  $("rec-digest").textContent = started.digest;
  uptime();
}

function uptime() {
  if (!state.started) return;
  const seconds = Math.max(0, Math.floor((now() - state.started.started) / 1_000));
  const [d, h, m] = [Math.floor(seconds / 86_400), Math.floor(seconds / 3_600) % 24, Math.floor(seconds / 60) % 60];
  $("uptime").textContent = d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m ${seconds % 60}s`;
}

// A second of statistics, as the engine room draws it.
const heatOf = (stats) => ({
  buckets: stats.turn_buckets || [],
  p50: stats.turn_p50_ns,
  p99: stats.turn_p99_ns,
  max: stats.turn_max_ns,
  cps: stats.commands_per_second,
});

const duration = (ns) => (ns >= 1_000_000
  ? `${(ns / 1_000_000).toFixed(2)} ms`
  : ns >= 1_000 ? `${Math.round(ns / 1_000)} µs` : `${ns} ns`);

function stats(message) {
  if (message.time) state.offset = message.time - Date.now();
  state.statsAt = Date.now();
  $("cps").textContent = count(message.commands_per_second);
  $("p50").textContent = duration(message.turn_p50_ns);
  $("p99").textContent = duration(message.turn_p99_ns);
  $("max").textContent = duration(message.turn_max_ns);
  $("sessions").textContent = count(message.sessions);
  $("resting").textContent = count(message.orders);
  $("heat-p50").textContent = duration(message.turn_p50_ns);
  $("heat-p99").textContent = duration(message.turn_p99_ns);
  $("heat-max").textContent = duration(message.turn_max_ns);
  $("tps-now").textContent = count(message.commands_per_second);
  stages(message);
  engine.heat.push(heatOf(message));
  if (engine.heat.length > SECONDS) engine.heat.shift();
  schedule("chart", "engine");
}

// The clock, and whether the engine is still talking.
setInterval(() => {
  $("clock").textContent = timeOf(now());
  uptime();
  const alive = state.connected && Date.now() - state.statsAt < 3_000;
  document.querySelector(".engine-state").classList.toggle("live", alive);
  $("engine-text").textContent = alive ? "Engine online" : "Engine unreachable";
}, 1_000);

// ---------- Notifications ----------

function toast(title, detail = "", kind = "info") {
  const element = document.createElement("div");
  element.className = `toast ${kind}`;
  const body = document.createElement("div");
  body.append(bold(title));
  if (detail) {
    const span = document.createElement("span");
    span.textContent = detail;
    body.append(span);
  }
  element.append(body);
  const toasts = $("toasts");
  toasts.prepend(element);
  while (toasts.children.length > 4) toasts.lastElementChild.remove();
  setTimeout(() => {
    element.classList.add("leaving");
    setTimeout(() => element.remove(), 200);
  }, 4_000);
}

// ---------- How it works ----------

const about = $("about");
function openAbout() {
  if (typeof about.showModal === "function") about.showModal();
}
$("about-open").addEventListener("click", openAbout);
about.addEventListener("click", (event) => {
  // A click on the backdrop closes it.
  if (event.target === about) about.close();
});
about.addEventListener("close", () => save("exchange-seen-about", true));

const renderers = {
  book: renderBook,
  trades: renderTrades,
  chart: drawChart,
  ticker: renderTicker,
  wallet: renderWallet,
  orders: renderOrders,
  ticket: updateTicket,
  engine: () => {
    drawHeat();
    drawTps();
  },
  leaders: renderLeaders,
};

for (const button of document.querySelectorAll("[data-view]")) {
  button.addEventListener("click", () => {
    chart.view = button.dataset.view;
    choose("[data-view]", "view", chart.view);
    $("price-tools").hidden = chart.view !== "price";
    schedule("chart");
  });
}
for (const button of document.querySelectorAll("[data-book]")) {
  button.addEventListener("click", () => setBookMode(button.dataset.book));
}
const resized = new ResizeObserver(() => {
  book.width = 0;
  schedule("engine", "book");
});
for (const id of ["heat", "tps", "asks"]) resized.observe($(id));

// The exchange logs out sessions that stay silent for five seconds.
setInterval(() => send({ type: "heartbeat" }), 2_000);
setSide("buy");
setBookMode(book.mode);
$("proof").hidden = Boolean(load("exchange-proof-hidden"));
$("proof-close").addEventListener("click", () => {
  $("proof").hidden = true;
  save("exchange-proof-hidden", true);
});
renderOrders();
renderLeaders();
if (!load("exchange-seen-about")) openAbout();
connect();
