// Zero-dependency tests (Node's built-in test runner): `npm test`
const test = require("node:test");
const assert = require("node:assert");
const { createOrder, computeKpis } = require("./index");

test("createOrder returns a consistent order", () => {
  const o = createOrder(7);
  assert.strictEqual(o.order_id, "LIVE-000007");
  assert.strictEqual(o.revenue, o.quantity * o.unit_price);
  assert.ok(o.quantity >= 1 && o.unit_price > 0);
});

test("computeKpis aggregates only the rolling window", () => {
  const now = 1_000_000;
  const orders = [
    { ts: now - 5_000, revenue: 1000, region: "North", category: "Home" },
    { ts: now - 10_000, revenue: 3000, region: "South", category: "Home" },
    { ts: now - 120_000, revenue: 99999, region: "East", category: "Fashion" }, // outside window
  ];
  const k = computeKpis(orders, now);
  assert.strictEqual(k.orders, 2);
  assert.strictEqual(k.revenue, 4000);
  assert.strictEqual(k.avg_order_value, 2000);
  assert.deepStrictEqual(k.by_region, { North: 1000, South: 3000 });
  assert.strictEqual(k.by_category.Fashion, undefined);
});

test("computeKpis handles an empty window", () => {
  const k = computeKpis([], Date.now());
  assert.strictEqual(k.orders, 0);
  assert.strictEqual(k.avg_order_value, 0);
});
