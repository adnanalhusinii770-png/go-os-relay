
require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { fetchPrice } = require('./providers');

const PORT = process.env.PORT || 8080;
const PROVIDER = process.env.PRICE_PROVIDER || 'twelvedata';
const API_KEY = process.env.PROVIDER_API_KEY;
const SYMBOL = process.env.SYMBOL || 'XAU/USD';
const POLL_INTERVAL_MS = (parseInt(process.env.POLL_INTERVAL_SECONDS, 10) || 5) * 1000;
const ACCESS_TOKEN = process.env.APP_ACCESS_TOKEN || null;

if (!API_KEY) {
  console.error('خطأ: لم يتم ضبط PROVIDER_API_KEY في متغيرات البيئة');
  process.exit(1);
}

const app = express();
app.get('/health', (req, res) => {
  res.json({ status: 'ok', provider: PROVIDER, symbol: SYMBOL, connectedClients: wss ? wss.clients.size : 0, lastPrice: lastKnownPrice });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

let lastKnownPrice = null;
let pollTimer = null;

function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach((client) => { if (client.readyState === client.OPEN) client.send(payload); });
}

async function pollPriceOnce() {
  try {
    const tick = await fetchPrice({ provider: PROVIDER, apiKey: API_KEY, symbol: SYMBOL });
    lastKnownPrice = tick;
    broadcast({ type: 'tick', symbol: tick.symbol, bid: tick.bid, ask: tick.ask, timestamp: tick.timestamp });
    console.log(`[${new Date().toLocaleTimeString()}] ${tick.symbol} -> Bid: ${tick.bid} | Ask: ${tick.ask}`);
  } catch (error) {
    console.error(`فشل جلب السعر: ${error.message}`);
  }
}

function startPolling() { pollPriceOnce(); pollTimer = setInterval(pollPriceOnce, POLL_INTERVAL_MS); }
function stopPolling() { if (pollTimer) clearInterval(pollTimer); }

wss.on('connection', (ws, req) => {
  if (ACCESS_TOKEN) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const token = url.searchParams.get('token');
    if (token !== ACCESS_TOKEN) { ws.close(4001, 'Unauthorized'); return; }
  }
  console.log(`عميل جديد متصل. الإجمالي: ${wss.clients.size}`);
  if (lastKnownPrice) {
    ws.send(JSON.stringify({ type: 'tick', symbol: lastKnownPrice.symbol, bid: lastKnownPrice.bid, ask: lastKnownPrice.ask, timestamp: lastKnownPrice.timestamp }));
  }
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.action === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
    } catch {}
  });
  ws.on('close', () => console.log(`عميل انقطع. الإجمالي: ${wss.clients.size}`));
  ws.on('error', (err) => console.error('خطأ عميل:', err.message));
});

server.listen(PORT, () => {
  console.log(`GO OS Price Relay Server يعمل على المنفذ ${PORT}`);
  startPolling();
});

process.on('SIGINT', () => {
  stopPolling();
  wss.close();
  server.close(() => process.exit(0));
});
