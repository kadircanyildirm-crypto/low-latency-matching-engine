"use strict";

// Prices are integer ticks of one cent; cash is ticks times lots.
const money = (ticks) => (ticks / 100).toLocaleString("en-US", { style: "currency", currency: "USD" });
const price = (ticks) => (ticks / 100).toFixed(2);
const clock = () => new Date().toLocaleTimeString("en-GB");
const $ = (id) => document.getElementById(id);

const state = {
  socket: null,
  account: null,
  side: "buy",
  bids: new Map(),
  asks: new Map(),
  trades: [],
  last: null,
  first: null,
  wallet: null,
  start: null,
  requests: new Map(), // client ref -> {side, price, qty}
  orders: new Map(), // order id -> {id, side, price, leaves}
  fills: [],
  nextRef: Date.now() % 1_000_000_000,
  retry: 500,
};

function credentials() {
  try {
    return JSON.parse(localStorage.getItem("exchange-account") || "null");
  } catch {
    return null;
  }
}

function send(message) {
  if (state.socket && state.socket.readyState === WebSocket.OPEN) {
    state.socket.send(JSON.stringify(message));
  }
}

function notice(text, error = false) {
  const element = $("notice");
  element.textContent = text;
  element.className = error ? "notice error" : "notice";
}

function connect() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/ws`);
  state.socket = socket;
  socket.onopen = () => {
    state.retry = 500;
    $("connection").textContent = "connected";
    $("connection").className = "pill on";
    const saved = credentials();
    if (saved) {
      send({ type: "login", account: saved.account, token: saved.token });
    } else {
      send({ type: "register" });
    }
  };
  socket.onmessage = (event) => receive(JSON.parse(event.data));
  socket.onclose = () => {
    $("connection").textContent = "reconnecting";
    $("connection").className = "pill off";
    state.orders.clear();
    renderOrders();
    setTimeout(connect, state.retry);
    state.retry = Math.min(state.retry * 2, 10_000);
  };
}

function receive(message) {
  switch (message.type) {
    case "registered":
      localStorage.setItem(
        "exchange-account",
        JSON.stringify({ account: message.account, token: message.token }),
      );
      send({ type: "login", account: message.account, token: message.token });
      break;
    case "login_accepted":
      state.account = message.account;
      $("account").textContent = `account #${message.account}`;
      send({ type: "subscribe" });
      break;
    case "login_rejected":
      // An account the exchange no longer knows, or one open in another tab.
      if (message.reason === "bad_credentials") {
        localStorage.removeItem("exchange-account");
        send({ type: "register" });
      } else {
        notice(`Login refused: ${message.reason.replaceAll("_", " ")}`, true);
      }
      break;
    case "book":
      state.bids.clear();
      state.asks.clear();
      break;
    case "level": {
      const side = message.side === "buy" ? state.bids : state.asks;
      if (message.orders === 0) side.delete(message.price);
      else side.set(message.price, { qty: message.qty, orders: message.orders });
      scheduleBook();
      break;
    }
    case "trade":
      state.trades.unshift({ time: clock(), price: message.price, qty: message.qty, side: message.side });
      state.trades.length = Math.min(state.trades.length, 60);
      state.last = message.price;
      if (state.first === null) state.first = message.price;
      pushPoint(message.price);
      scheduleTrades();
      renderWallet();
      break;
    case "report":
      report(message);
      break;
    case "balance":
      state.wallet = message;
      if (state.start === null) state.start = message;
      renderWallet();
      break;
    case "reject":
      state.requests.delete(message.ref);
      notice(`Refused: ${message.reason.replaceAll("_", " ")}`, true);
      break;
    case "logout":
      notice(`Logged out: ${message.reason.replaceAll("_", " ")}`, true);
      break;
    case "error":
      notice(message.message, true);
      break;
    default:
      break;
  }
}

function report(message) {
  const known = state.orders.get(message.id);
  switch (message.kind) {
    case "accepted": {
      const request = state.requests.get(message.ref);
      state.requests.delete(message.ref);
      if (request) state.orders.set(message.id, { id: message.id, ...request, leaves: request.qty });
      break;
    }
    case "rested":
      state.orders.set(message.id, {
        id: message.id,
        side: message.side,
        price: message.price,
        leaves: message.qty,
      });
      break;
    case "fill":
      state.fills.unshift({ time: clock(), side: message.side, price: message.price, qty: message.qty });
      state.fills.length = Math.min(state.fills.length, 30);
      if (message.leaves === 0) state.orders.delete(message.id);
      else if (known) known.leaves = message.leaves;
      notice(`${message.side === "buy" ? "Bought" : "Sold"} ${message.qty} at ${price(message.price)}`);
      break;
    case "cancelled":
      state.orders.delete(message.id);
      break;
    case "rejected":
      state.orders.delete(message.id);
      notice(`Refused by the book: ${message.reason.replaceAll("_", " ")}`, true);
      break;
    case "phase_changed":
      $("phase").textContent = message.phase;
      $("phase").hidden = false;
      break;
    default:
      break;
  }
  renderOrders();
}

// The book is drawn at most once a frame.
let bookPending = false;
function scheduleBook() {
  if (bookPending) return;
  bookPending = true;
  requestAnimationFrame(() => {
    bookPending = false;
    renderBook();
  });
}

function rows(levels, element, side, depth) {
  const max = Math.max(1, ...levels.map(([, level]) => level.qty));
  element.replaceChildren(
    ...levels.slice(0, depth).map(([at, level]) => {
      const row = document.createElement("div");
      const bar = document.createElement("i");
      bar.className = "bar";
      bar.style.width = `${(100 * level.qty) / max}%`;
      const cells = [price(at), level.qty, level.orders].map((text, index) => {
        const cell = document.createElement("span");
        cell.textContent = text;
        if (index === 0) cell.className = "price";
        return cell;
      });
      row.append(bar, ...cells);
      // Clicking a level sets up the order that would trade with it.
      row.onclick = () => {
        $("price").value = price(at);
        setSide(side === "ask" ? "buy" : "sell");
      };
      return row;
    }),
  );
}

function renderBook() {
  const bids = [...state.bids].sort((a, b) => b[0] - a[0]);
  const asks = [...state.asks].sort((a, b) => a[0] - b[0]);
  rows(asks.slice(0, 12).reverse(), $("asks"), "ask", 12);
  rows(bids, $("bids"), "bid", 12);
  const spread = bids.length && asks.length ? asks[0][0] - bids[0][0] : null;
  $("spread").textContent = spread === null ? "—" : `spread ${price(spread)}`;
  if (!$("price").value && bids.length && asks.length) {
    $("price").value = price(Math.round((bids[0][0] + asks[0][0]) / 2));
  }
}

let tradesPending = false;
function scheduleTrades() {
  if (tradesPending) return;
  tradesPending = true;
  requestAnimationFrame(() => {
    tradesPending = false;
    $("trades").replaceChildren(
      ...state.trades.slice(0, 30).map((trade) => {
        const row = document.createElement("div");
        row.className = trade.side;
        for (const text of [trade.time, price(trade.price), trade.qty]) {
          const cell = document.createElement("span");
          cell.textContent = text;
          row.append(cell);
        }
        return row;
      }),
    );
    $("last").textContent = state.last === null ? "—" : price(state.last);
    if (state.first !== null && state.last !== null) {
      const change = ((state.last - state.first) / state.first) * 100;
      $("change").textContent = `${change >= 0 ? "+" : ""}${change.toFixed(2)}% since you opened this page`;
    }
    drawChart();
  });
}

// The chart keeps a point a second: the last trade price in it.
const points = [];
function pushPoint(at) {
  const second = Math.floor(Date.now() / 1000);
  const lastPoint = points[points.length - 1];
  if (lastPoint && lastPoint.second === second) lastPoint.price = at;
  else points.push({ second, price: at });
  if (points.length > 600) points.shift();
}

function drawChart() {
  const canvas = $("canvas");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (canvas.width !== width * ratio) {
    canvas.width = width * ratio;
    canvas.height = height * ratio;
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.clearRect(0, 0, width, height);
  if (points.length < 2) return;
  const prices = points.map((p) => p.price);
  const low = Math.min(...prices);
  const high = Math.max(...prices);
  const span = Math.max(high - low, 1);
  const x = (index) => 50 + (index / (points.length - 1)) * (width - 60);
  const y = (at) => 10 + (1 - (at - low) / span) * (height - 30);
  context.strokeStyle = "#262d36";
  context.fillStyle = "#8b949e";
  context.font = "11px system-ui";
  for (const at of [low, (low + high) / 2, high]) {
    context.beginPath();
    context.moveTo(50, y(at));
    context.lineTo(width - 10, y(at));
    context.stroke();
    context.fillText(price(Math.round(at)), 2, y(at) + 4);
  }
  const rising = prices[prices.length - 1] >= prices[0];
  context.strokeStyle = rising ? "#2ea043" : "#f85149";
  context.lineWidth = 1.5;
  context.beginPath();
  points.forEach((point, index) => {
    if (index === 0) context.moveTo(x(index), y(point.price));
    else context.lineTo(x(index), y(point.price));
  });
  context.stroke();
}

function renderWallet() {
  const wallet = state.wallet;
  if (!wallet) return;
  $("cash").textContent = money(wallet.cash);
  $("position").textContent = wallet.position.toLocaleString("en-US");
  $("held").textContent = `${money(wallet.cash_held)} · ${wallet.position_held} shares`;
  if (state.last !== null && state.start) {
    const value = wallet.cash + wallet.position * state.last;
    const start = state.start.cash + state.start.position * state.last;
    $("value").textContent = money(value);
    const pnl = value - start;
    $("pnl").textContent = `${pnl >= 0 ? "+" : ""}${money(pnl)}`;
    $("pnl").className = pnl >= 0 ? "up" : "down";
  }
}

function renderOrders() {
  const orders = [...state.orders.values()].sort((a, b) => b.id - a.id);
  $("open-orders").replaceChildren(
    ...orders.map((order) => {
      const row = document.createElement("tr");
      const cells = [order.id, order.side, price(order.price), order.leaves];
      cells.forEach((text, index) => {
        const cell = document.createElement("td");
        cell.textContent = text;
        if (index === 1) cell.className = order.side;
        row.append(cell);
      });
      const action = document.createElement("td");
      const cancel = document.createElement("button");
      cancel.className = "link";
      cancel.textContent = "cancel";
      cancel.onclick = () => send({ type: "cancel", id: order.id });
      action.append(cancel);
      row.append(action);
      return row;
    }),
  );
  $("fills").replaceChildren(
    ...state.fills.map((fill) => {
      const row = document.createElement("tr");
      [fill.time, fill.side, price(fill.price), fill.qty].forEach((text, index) => {
        const cell = document.createElement("td");
        cell.textContent = text;
        if (index === 1) cell.className = fill.side;
        row.append(cell);
      });
      return row;
    }),
  );
}

function setSide(side) {
  state.side = side;
  $("buy-side").classList.toggle("active", side === "buy");
  $("sell-side").classList.toggle("active", side === "sell");
  $("submit").className = `submit ${side}`;
  $("submit").textContent = side === "buy" ? "Buy" : "Sell";
  $("now").textContent = side === "buy" ? "Buy now at the best price" : "Sell now at the best price";
}

function place(ticks, tif) {
  const qty = Math.floor(Number($("qty").value));
  if (!Number.isFinite(ticks) || ticks <= 0 || !(qty > 0)) {
    notice("Enter a price and a quantity", true);
    return;
  }
  const ref = ++state.nextRef;
  state.requests.set(ref, { side: state.side, price: ticks, qty });
  send({ type: "order", ref, side: state.side, qty, price: ticks, tif });
  notice("");
}

$("buy-side").onclick = () => setSide("buy");
$("sell-side").onclick = () => setSide("sell");
$("submit").onclick = () => place(Math.round(Number($("price").value) * 100), $("tif").value);
$("now").onclick = () => {
  // A limit order at the best opposite price, immediate or cancel: paper accounts hold
  // what an order can cost, so there are no market orders.
  const opposite = state.side === "buy"
    ? Math.min(...state.asks.keys())
    : Math.max(...state.bids.keys());
  if (!Number.isFinite(opposite)) {
    notice("Nobody is quoting that side right now", true);
    return;
  }
  place(opposite, "ioc");
};
$("cancel-all").onclick = () => send({ type: "cancel_all" });

// The exchange logs out sessions that stay silent for five seconds.
setInterval(() => send({ type: "heartbeat" }), 2_000);
window.addEventListener("resize", drawChart);
setSide("buy");
connect();
