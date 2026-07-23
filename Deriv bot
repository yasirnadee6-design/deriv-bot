/**
 * Deriv 24/7 auto-trade bot (Node.js, runs headless on a server)
 * ----------------------------------------------------------------
 * Same BB/RSI signal logic as the browser version, but designed to
 * run continuously on a VPS / cloud worker instead of in a browser tab.
 *
 * SETUP
 *   npm init -y
 *   npm install ws dotenv
 *   node signal-bot-server.js
 *
 * CONFIG: put these in a .env file next to this script (never hardcode
 * your token in the file itself):
 *   DERIV_TOKEN=your_api_token
 *   DERIV_APP_ID=1089
 *   SYMBOL=R_100
 *   STAKE=1
 *   DURATION_TICKS=5
 *   AUTO_TRADE=false        # set to true only after testing on demo
 */

require("dotenv").config();
const WebSocket = require("ws");

const TOKEN = process.env.DERIV_TOKEN;
const APP_ID = process.env.DERIV_APP_ID || "1089";
const SYMBOL = process.env.SYMBOL || "R_100";
const STAKE = Number(process.env.STAKE || 1);
const DURATION_TICKS = Number(process.env.DURATION_TICKS || 5);
const AUTO_TRADE = process.env.AUTO_TRADE === "true";

if (!TOKEN) {
  console.error("Missing DERIV_TOKEN in .env — get one from Deriv > Settings > API Token.");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Indicators
// ---------------------------------------------------------------------------
function bollinger(series, window = 20, mult = 2) {
  if (series.length < window) return { upper: null, lower: null };
  const slice = series.slice(-window);
  const mean = slice.reduce((a, b) => a + b, 0) / window;
  const variance = slice.reduce((a, b) => a + (b - mean) ** 2, 0) / window;
  const sd = Math.sqrt(variance);
  return { upper: mean + mult * sd, lower: mean - mult * sd };
}

function rsi(series, window = 14) {
  if (series.length < window + 1) return null;
  const slice = series.slice(-(window + 1));
  let gains = 0, losses = 0;
  for (let i = 1; i < slice.length; i++) {
    const d = slice[i] - slice[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  const avgGain = gains / window;
  const avgLoss = losses / window;
  if (avgLoss === 0) return 100;
  return 100 - 100 / (1 + avgGain / avgLoss);
}

// ---------------------------------------------------------------------------
// Bot state
// ---------------------------------------------------------------------------
let prices = [];
let account = null;
let lastSignal = { bb: "HOLD", rsi: "HOLD" };
let pending = false;
let ws;

function connect() {
  ws = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`);

  ws.on("open", () => {
    log("connected — authorizing…");
    send({ authorize: TOKEN });
  });

  ws.on("message", (raw) => handle(JSON.parse(raw)));

  ws.on("close", () => {
    log("disconnected — reconnecting in 5s…");
    setTimeout(connect, 5000);
  });

  ws.on("error", (err) => log("ws error: " + err.message));
}

function send(obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function handle(data) {
  if (data.error) {
    log("API error:", data.error.message);
    pending = false;
    return;
  }

  switch (data.msg_type) {
    case "authorize":
      account = {
        loginid: data.authorize.loginid,
        currency: data.authorize.currency,
        is_virtual: !!data.authorize.is_virtual,
      };
      log(`authorized: ${account.loginid} (${account.is_virtual ? "DEMO" : "REAL"}), balance ${data.authorize.balance} ${account.currency}`);
      if (!account.is_virtual && AUTO_TRADE) {
        log("*** WARNING: AUTO_TRADE is enabled on a REAL account. Trading with real funds. ***");
      }
      send({ ticks: SYMBOL, subscribe: 1 });
      break;

    case "tick":
      prices.push(data.tick.quote);
      if (prices.length > 200) prices.shift();
      evaluateSignals();
      break;

    case "proposal":
      send({ buy: data.proposal.id, price: data.proposal.ask_price });
      break;

    case "buy":
      log(`BOUGHT contract ${data.buy.contract_id} — price ${data.buy.buy_price}, payout ${data.buy.payout}`);
      pending = false;
      send({ proposal_open_contract: 1, contract_id: data.buy.contract_id, subscribe: 1 });
      break;

    case "proposal_open_contract":
      if (data.proposal_open_contract.is_sold) {
        const p = data.proposal_open_contract.profit;
        log(`CONTRACT ${data.proposal_open_contract.contract_id} closed — ${p >= 0 ? "WON" : "LOST"} (${p})`);
      }
      break;

    default:
      break;
  }
}

function evaluateSignals() {
  const bb = bollinger(prices);
  const r = rsi(prices);
  const price = prices[prices.length - 1];

  const bbSignal = bb.upper == null ? "HOLD" : price > bb.upper ? "BUY" : price < bb.lower ? "SELL" : "HOLD";
  const rsiSignal = r == null ? "HOLD" : r < 30 ? "BUY" : r > 70 ? "SELL" : "HOLD";

  if (AUTO_TRADE && account && !pending) {
    if (bbSignal !== "HOLD" && bbSignal !== lastSignal.bb) placeTrade(bbSignal, "15s/BB");
    else if (rsiSignal !== "HOLD" && rsiSignal !== lastSignal.rsi) placeTrade(rsiSignal, "1m/RSI");
  }

  lastSignal = { bb: bbSignal, rsi: rsiSignal };
}

function placeTrade(direction, source) {
  pending = true;
  log(`SIGNAL (${source}): ${direction} — requesting proposal…`);
  send({
    proposal: 1,
    amount: STAKE,
    basis: "stake",
    contract_type: direction === "BUY" ? "CALL" : "PUT",
    currency: account.currency,
    duration: DURATION_TICKS,
    duration_unit: "t",
    symbol: SYMBOL,
  });
}

log(`starting bot — symbol=${SYMBOL} stake=${STAKE} duration=${DURATION_TICKS}t auto_trade=${AUTO_TRADE}`);
connect();
