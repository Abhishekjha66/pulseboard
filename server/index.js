/**
 * PulseBoard live server
 *  - Serves the dashboard (../docs) and the Gold-layer JSON
 *  - Streams simulated order events over Socket.IO (event-driven, real-time)
 *  - Exposes REST endpoints for rolling KPIs
 *
 * Event flow:  generator --(order:new)--> all clients
 *              server    --(kpi:update)--> all clients (every second, rolling 60s window)
 */
const path = require("path");
const http = require("http");
const express = require("express");
const cors = require("cors");
const { Server } = require("socket.io");

const PRODUCTS = [
  ["Laptop", "Electronics", 55000], ["Smartphone", "Electronics", 24000],
  ["Headphones", "Electronics", 3500], ["Smartwatch", "Electronics", 8000],
  ["Running Shoes", "Fashion", 4200], ["Backpack", "Fashion", 1800],
  ["Jacket", "Fashion", 3200], ["Coffee Maker", "Home", 5200],
  ["Desk Lamp", "Home", 1400], ["Office Chair", "Home", 9500],
  ["Yoga Mat", "Sports", 1100], ["Dumbbell Set", "Sports", 3800],
];
const REGIONS = ["North", "South", "East", "West"];
const CHANNELS = ["Web", "Mobile App", "Marketplace"];
const WINDOW_MS = 60_000;

const pick = (a) => a[Math.floor(Math.random() * a.length)];

function createOrder(seq) {
  const [product, category, base] = pick(PRODUCTS);
  const quantity = [1, 1, 1, 1, 1, 1, 1, 2, 2, 3][Math.floor(Math.random() * 10)];
  const unit_price = Math.round(base * (0.9 + Math.random() * 0.2));
  return {
    order_id: `LIVE-${String(seq).padStart(6, "0")}`,
    ts: Date.now(),
    product, category,
    region: pick(REGIONS),
    channel: pick(CHANNELS),
    quantity, unit_price,
    revenue: quantity * unit_price,
  };
}

/** Rolling-window KPI calculator (pure function -> easy to unit test). */
function computeKpis(orders, now = Date.now()) {
  const recent = orders.filter((o) => now - o.ts <= WINDOW_MS);
  const revenue = recent.reduce((s, o) => s + o.revenue, 0);
  const byRegion = {};
  const byCategory = {};
  for (const o of recent) {
    byRegion[o.region] = (byRegion[o.region] || 0) + o.revenue;
    byCategory[o.category] = (byCategory[o.category] || 0) + o.revenue;
  }
  return {
    window_seconds: WINDOW_MS / 1000,
    orders: recent.length,
    revenue,
    avg_order_value: recent.length ? Math.round(revenue / recent.length) : 0,
    orders_per_minute: recent.length,
    by_region: byRegion,
    by_category: byCategory,
  };
}

function createApp() {
  const app = express();
  app.use(cors());
  const state = { orders: [], seq: 0 };

  app.get("/api/health", (_req, res) => res.json({ status: "ok", uptime_s: Math.round(process.uptime()) }));
  app.get("/api/kpis", (_req, res) => res.json(computeKpis(state.orders)));
  app.get("/api/orders/recent", (req, res) => {
    const n = Math.min(parseInt(req.query.limit, 10) || 20, 100);
    res.json(state.orders.slice(-n).reverse());
  });
  app.use("/data", express.static(path.join(__dirname, "..", "docs", "data")));
  app.use(express.static(path.join(__dirname, "..", "docs")));
  return { app, state };
}

function start(port = process.env.PORT || 3000) {
  const { app, state } = createApp();
  const server = http.createServer(app);
  const io = new Server(server, { cors: { origin: "*" } });

  io.on("connection", (socket) => {
    socket.emit("kpi:update", computeKpis(state.orders));
    socket.emit("order:history", state.orders.slice(-15));
  });

  // Order generator: Poisson-ish arrivals (~0.5-2.5 orders/sec)
  (function emitLoop() {
    const order = createOrder(++state.seq);
    state.orders.push(order);
    const cutoff = Date.now() - 5 * WINDOW_MS;
    while (state.orders.length && state.orders[0].ts < cutoff) state.orders.shift();
    io.emit("order:new", order);
    setTimeout(emitLoop, 400 + Math.random() * 1600);
  })();

  setInterval(() => io.emit("kpi:update", computeKpis(state.orders)), 1000);

  return new Promise((resolve) =>
    server.listen(port, () => {
      console.log(`PulseBoard live on http://localhost:${port}`);
      resolve({ server, io, state });
    })
  );
}

if (require.main === module) start();
module.exports = { createOrder, computeKpis, createApp, start };
