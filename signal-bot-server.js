    // Deriv 24/7 auto-trade bot v4 -- robust response parsing, logs raw API responses
// Deriv 24/7 auto-trade bot -- uses Deriv new REST + OTP auth flow
// Setup: npm init -y && npm install ws dotenv
// Then: node signal-bot-server.js
// .env needs: DERIV_TOKEN, DERIV_APP_ID, ACCOUNT_TYPE, SYMBOL, STAKE, DURATION_TICKS, AUTO_TRADE

require("dotenv").config();
const WebSocket = require("ws");

const TOKEN = process.env.DERIV_TOKEN;
const APP_ID = process.env.DERIV_APP_ID || "1089";
const ACCOUNT_TYPE = (process.env.ACCOUNT_TYPE || "demo").toLowerCase(); // demo | real
const SYMBOL = process.env.SYMBOL || "R_100";
const STAKE = Number(process.env.STAKE || 1);
const DURATION_TICKS = Number(process.env.DURATION_TICKS || 5);
const AUTO_TRADE = process.env.AUTO_TRADE === "true";

const REST_BASE = "https://api.derivws.com/trading/v1/options";

if (!TOKEN) {
  console.error("Missing DERIV_TOKEN in .env — get one from Deriv > Settings > API Token.");
  process.exit(1);
}

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

// ---------------------------------------------------------------------------
// Step 1 + 2: REST calls to get an account and then an OTP WebSocket URL
// ---------------------------------------------------------------------------
async function safeJson(res) {
  const text = await res.text();
  log(`raw response (status ${res.status}):`, text.slice(0, 500));
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`response was not JSON (status ${res.status}): ${text.slice(0, 200)}`);
  }
}

async function getAccounts() {
  const res = await fetch(`${REST_BASE}/accounts`, {
    headers: {
      "Deriv-App-ID": APP_ID,
      "Authorization": `Bearer ${TOKEN}`,
    },
  });
  const body = await safeJson(res);
  if (!res.ok) {
    throw new Error(`accounts request failed (${res.status}): ${JSON.stringify(body)}`);
  }
  // handle whichever shape the API actually returns
  const list = body.accounts || body.data || body;
  return Array.isArray(list) ? list : [list];
}

async function getWsUrl(accountId) {
  const res = await fetch(`${REST_BASE}/accounts/${accountId}/otp`, {
    method: "POST",
    headers: {
      "Deriv-App-ID": APP_ID,
      "Authorization": `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
  });
  const body = await safeJson(res);
  if (!res.ok) {
    throw new Error(`otp request failed (${res.status}): ${JSON.stringify(body)}`);
  }
  // handle whichever shape the API actually returns
  return body.ws_url || body.url || (body.data && body.data.url);
}

// ---------------------------------------------------------------------------
// Indicators (unchanged)
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

async function connect() {
  try {
    log("fetching accounts…");
    const accounts = await getAccounts();
    log("accounts:", accounts.map((a) => `${a.account_id}(${a.account_type})`).join(", "));

    log("accounts raw:", JSON.stringify(accounts));
    const accType = (a) => a.account_type || a.accountType || a.type || "";
    const accId = (a) => a.account_id || a.accountId || a.id || a.loginid;
    const chosen = accounts.find((a) => accType(a).toLowerCase() === ACCOUNT_TYPE) || accounts[0];
    if (!chosen) throw new Error("no accounts returned");
    account = { id: accId(chosen), currency: chosen.currency || "USD", is_demo: accType(chosen).toLowerCase() === "demo" };
    log(`using account ${account.id} (${accType(chosen)}), balance ${chosen.balance} ${account.currency}`);

    log("requesting OTP / websocket url…");
    const wsUrl = await getWsUrl(account.id);

    ws = new WebSocket(wsUrl);
    ws.on("open", () => {
      log("connected via OTP — subscribing to ticks…");
      send({ ticks: SYMBOL, subscribe: 1 });
    });
    ws.on("message", (raw) => handle(JSON.parse(raw)));
    ws.on("close", () => {
      log("disconnected — reconnecting in 5s…");
      setTimeout(connect, 5000);
    });
    ws.on("error", (err) => log("ws error:", err.message));
  } catch (err) {
    log("connect() failed:", err.message);
    log("retrying in 10s…");
    setTimeout(connect, 10000);
  }
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function handle(data) {
  if (data.error) {
    log("API error:", data.error.message);
    pending = false;
    return;
  }

  switch (data.msg_type) {
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

log(`starting bot — symbol=${SYMBOL} stake=${STAKE} duration=${DURATION_TICKS}t account_type=${ACCOUNT_TYPE} auto_trade=${AUTO_TRADE}`);
connect();
