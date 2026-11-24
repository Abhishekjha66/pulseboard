"""Generate a realistic, deliberately messy raw orders dataset (the Bronze source).

Dirty data injected on purpose so the Silver layer has real work to do:
duplicates, missing values, inconsistent casing, mixed date formats,
negative quantities and price outliers.
"""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

PRODUCTS = {
    "Laptop": ("Electronics", 55000),
    "Smartphone": ("Electronics", 24000),
    "Headphones": ("Electronics", 3500),
    "Smartwatch": ("Electronics", 8000),
    "Running Shoes": ("Fashion", 4200),
    "Backpack": ("Fashion", 1800),
    "Jacket": ("Fashion", 3200),
    "Coffee Maker": ("Home", 5200),
    "Desk Lamp": ("Home", 1400),
    "Office Chair": ("Home", 9500),
    "Yoga Mat": ("Sports", 1100),
    "Dumbbell Set": ("Sports", 3800),
}
REGIONS = ["North", "South", "East", "West"]
CITIES = {
    "North": ["Delhi", "Chandigarh", "Lucknow"],
    "South": ["Bengaluru", "Chennai", "Hyderabad"],
    "East": ["Kolkata", "Patna", "Bhubaneswar"],
    "West": ["Mumbai", "Pune", "Ahmedabad"],
}
CHANNELS = ["Web", "Mobile App", "Marketplace"]


def main(rows: int, out: Path, seed: int) -> None:
    rnd = random.Random(seed)
    start = datetime(2026, 1, 1)
    days = 300
    out.parent.mkdir(parents=True, exist_ok=True)
    records = []

    # Per-day demand = growth trend x weekday seasonality x daily noise, so the
    # forecasting model has genuine structure to learn (and be honestly scored on).
    weekday_factor = [0.85, 0.9, 0.95, 1.0, 1.15, 1.35, 1.25]  # Mon..Sun
    day_weights = [
        (1 + 1.2 * d / days)
        * weekday_factor[(start + timedelta(days=d)).weekday()]
        * rnd.uniform(0.85, 1.15)
        for d in range(days)
    ]
    day_choices = rnd.choices(range(days), weights=day_weights, k=rows)

    for i in range(1, rows + 1):
        day = day_choices[i - 1]
        ts = start + timedelta(days=day, seconds=rnd.randint(0, 86399))
        product = rnd.choice(list(PRODUCTS))
        category, base = PRODUCTS[product]
        region = rnd.choice(REGIONS)
        price = round(base * rnd.uniform(0.9, 1.1), 2)
        qty = rnd.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        records.append(
            {
                "order_id": f"ORD-{100000 + i}",
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": f"C{rnd.randint(1, rows // 4):05d}",
                "product": product,
                "category": category,
                "region": region,
                "city": rnd.choice(CITIES[region]),
                "channel": rnd.choice(CHANNELS),
                "quantity": qty,
                "unit_price": price,
            }
        )

    # ---- inject dirt (~10% of rows) ----
    n = len(records)
    for r in rnd.sample(records, int(n * 0.02)):
        r["quantity"] = ""  # missing quantity
    for r in rnd.sample(records, int(n * 0.02)):
        r["unit_price"] = ""  # missing price
    for r in rnd.sample(records, int(n * 0.015)):
        r["quantity"] = -abs(int(r["quantity"] or 1))  # negative qty
    for r in rnd.sample(records, int(n * 0.01)):
        r["unit_price"] = 0  # zero price
    for r in rnd.sample(records, int(n * 0.01)):
        r["unit_price"] = round(float(r["unit_price"] or 1000) * 40, 2)  # outlier
    for r in rnd.sample(records, int(n * 0.03)):
        r["region"] = r["region"].lower()  # inconsistent casing
    for r in rnd.sample(records, int(n * 0.02)):
        r["region"] = f" {r['region']} "  # stray whitespace
    for r in rnd.sample(records, int(n * 0.02)):
        r["product"] = r["product"].upper()
    for r in rnd.sample(records, int(n * 0.02)):
        d = datetime.strptime(r["order_ts"], "%Y-%m-%d %H:%M:%S")
        r["order_ts"] = d.strftime("%d/%m/%Y %H:%M")  # mixed date format
    for r in rnd.sample(records, int(n * 0.01)):
        r["order_ts"] = ""  # missing timestamp
    # exact duplicate rows
    records += [dict(r) for r in rnd.sample(records, int(n * 0.03))]
    rnd.shuffle(records)

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        w.writeheader()
        w.writerows(records)
    print(f"Wrote {len(records):,} raw rows -> {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--out", type=Path, default=Path("data/raw_orders.csv"))
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    main(a.rows, a.out, a.seed)
