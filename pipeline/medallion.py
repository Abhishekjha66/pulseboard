"""Medallion ETL: Bronze (raw) -> Silver (clean) -> Gold (business aggregates).

Runs on SQLite + pandas so anyone can clone and run it with zero infrastructure.
The SQL in the Silver/Gold layers is plain ANSI-style and ports directly to
Snowflake / BigQuery / Postgres.

Outputs
-------
data/pulseboard.db          SQLite warehouse (bronze_orders, silver_orders, gold_* tables)
docs/data/gold.json         Gold layer + forecast + data-quality report for the dashboard
"""
import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score

KNOWN_PRODUCTS = None  # filled from the data after normalisation


# --------------------------------------------------------------------------- #
# BRONZE: land raw data as-is, add lineage columns
# --------------------------------------------------------------------------- #
def load_bronze(con: sqlite3.Connection, csv_path: Path) -> int:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    df["_source_file"] = csv_path.name
    df.to_sql("bronze_orders", con, if_exists="replace", index=False)
    return len(df)


# --------------------------------------------------------------------------- #
# SILVER: clean, standardise, de-duplicate, validate
# --------------------------------------------------------------------------- #
def build_silver(con: sqlite3.Connection) -> dict:
    df = pd.read_sql("SELECT * FROM bronze_orders", con)
    report = {"bronze_rows": len(df), "rejected": {}}

    # 1. exact + business-key duplicates
    before = len(df)
    df = df.drop_duplicates(subset=[c for c in df.columns if not c.startswith("_")])
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    report["rejected"]["duplicate_orders"] = before - len(df)

    # 2. standardise text
    for col in ["region", "city", "channel", "category"]:
        df[col] = df[col].str.strip().str.title()
    df["product"] = df["product"].str.strip().str.title()
    df["customer_id"] = df["customer_id"].str.strip().str.upper()

    # 3. parse timestamps (two known formats)
    ts = pd.to_datetime(df["order_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    alt = pd.to_datetime(df["order_ts"], format="%d/%m/%Y %H:%M", errors="coerce")
    df["order_ts"] = ts.fillna(alt)
    report["fixed_mixed_date_formats"] = int(alt.notna().sum())

    # 4. numeric coercion
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # 5. validation rules -> reject with a reason
    def reject(mask: pd.Series, reason: str) -> None:
        nonlocal df
        report["rejected"][reason] = int(mask.sum())
        df = df[~mask]

    reject(df["order_ts"].isna(), "missing_or_invalid_timestamp")
    reject(df["quantity"].isna() | df["unit_price"].isna(), "missing_quantity_or_price")
    reject(df["quantity"] <= 0, "non_positive_quantity")
    reject(df["unit_price"] <= 0, "non_positive_price")

    # 6. outlier rule: price > 5x the product median is a data-entry error
    med = df.groupby("product")["unit_price"].transform("median")
    reject(df["unit_price"] > 5 * med, "price_outlier_gt_5x_median")

    df["quantity"] = df["quantity"].astype(int)
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    df["order_date"] = df["order_ts"].dt.strftime("%Y-%m-%d")
    df["order_hour"] = df["order_ts"].dt.hour
    df["order_ts"] = df["order_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.drop(columns=["_source_file"])

    df.to_sql("silver_orders", con, if_exists="replace", index=False)
    report["silver_rows"] = len(df)
    report["rejection_rate_pct"] = round(
        100 * (1 - len(df) / report["bronze_rows"]), 2
    )
    return report


# --------------------------------------------------------------------------- #
# GOLD: business-ready aggregates (pure SQL)
# --------------------------------------------------------------------------- #
GOLD_SQL = {
    "gold_daily_sales": """
        SELECT order_date AS date,
               COUNT(*)                         AS orders,
               SUM(quantity)                    AS units,
               ROUND(SUM(revenue), 2)           AS revenue,
               ROUND(AVG(revenue), 2)           AS avg_order_value
        FROM silver_orders GROUP BY order_date ORDER BY order_date""",
    "gold_product_summary": """
        SELECT product, category,
               COUNT(*) AS orders, SUM(quantity) AS units,
               ROUND(SUM(revenue), 2) AS revenue
        FROM silver_orders GROUP BY product, category ORDER BY revenue DESC""",
    "gold_region_summary": """
        SELECT region, COUNT(*) AS orders, ROUND(SUM(revenue), 2) AS revenue,
               ROUND(AVG(revenue), 2) AS avg_order_value
        FROM silver_orders GROUP BY region ORDER BY revenue DESC""",
    "gold_channel_summary": """
        SELECT channel, COUNT(*) AS orders, ROUND(SUM(revenue), 2) AS revenue
        FROM silver_orders GROUP BY channel ORDER BY revenue DESC""",
    "gold_hourly_pattern": """
        SELECT order_hour AS hour, COUNT(*) AS orders
        FROM silver_orders GROUP BY order_hour ORDER BY order_hour""",
    "gold_monthly_sales": """
        SELECT substr(order_date, 1, 7) AS month,
               COUNT(*) AS orders, ROUND(SUM(revenue), 2) AS revenue
        FROM silver_orders GROUP BY month ORDER BY month""",
}


def build_gold(con: sqlite3.Connection) -> dict:
    out = {}
    for name, sql in GOLD_SQL.items():
        df = pd.read_sql(sql, con)
        df.to_sql(name, con, if_exists="replace", index=False)
        out[name] = df
    return out


# --------------------------------------------------------------------------- #
# ML: 30-day revenue forecast (linear regression on trend + weekday features)
# --------------------------------------------------------------------------- #
def forecast_revenue(daily: pd.DataFrame, horizon: int = 30) -> dict:
    """Forecast revenue as (predicted daily orders) x (average order value).

    Raw daily revenue is dominated by a few big-ticket orders (a laptop is ~50x a
    yoga mat), which makes it very noisy. Order volume has clear trend + weekday
    structure, so we model that with linear regression and scale by the average
    order value. Everything is scored on a chronological hold-out (last 20%).
    """
    d = daily.copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.set_index("date").asfreq("D", fill_value=0).reset_index()
    d = d[d["orders"] > 0].reset_index(drop=True)

    def features(dates: pd.Series, t0: pd.Timestamp) -> pd.DataFrame:
        X = pd.DataFrame({"t": (dates - t0).dt.days})
        for k in range(1, 7):
            X[f"dow{k}"] = (dates.dt.dayofweek == k).astype(int)
        return X

    t0 = d["date"].min()
    X = features(d["date"], t0)
    orders, revenue = d["orders"].values.astype(float), d["revenue"].values

    # ---- honest evaluation: train on first 80%, test on last 20% ----
    split = int(len(d) * 0.8)
    m_eval = LinearRegression().fit(X[:split], orders[:split])
    aov_train = revenue[:split].sum() / orders[:split].sum()
    pred_orders = m_eval.predict(X[split:])
    pred_rev = pred_orders * aov_train
    metrics = {
        "orders_r2": round(float(r2_score(orders[split:], pred_orders)), 3),
        "revenue_r2": round(float(r2_score(revenue[split:], pred_rev)), 3),
        "revenue_mae": round(float(mean_absolute_error(revenue[split:], pred_rev)), 2),
        "revenue_mape_pct": round(
            float(np.mean(np.abs((revenue[split:] - pred_rev) / revenue[split:])) * 100), 2
        ),
        "train_days": split,
        "test_days": len(d) - split,
    }

    # ---- refit on everything and forecast ahead ----
    model = LinearRegression().fit(X, orders)
    aov = revenue.sum() / orders.sum()
    future_dates = pd.Series(
        pd.date_range(d["date"].max() + pd.Timedelta(days=1), periods=horizon)
    )
    pred = np.maximum(model.predict(features(future_dates, t0)), 0) * aov
    resid_std = float(np.std(revenue - model.predict(X) * aov))

    return {
        "metrics": metrics,
        "orders_trend_per_day": round(float(model.coef_[0]), 3),
        "avg_order_value": round(float(aov), 2),
        "forecast": [
            {
                "date": dt.strftime("%Y-%m-%d"),
                "revenue": round(float(p), 2),
                "lower": round(float(max(p - 1.96 * resid_std, 0)), 2),
                "upper": round(float(p + 1.96 * resid_std), 2),
            }
            for dt, p in zip(future_dates, pred)
        ],
    }


def main(csv_path: Path, db_path: Path, out_json: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)

    n = load_bronze(con, csv_path)
    print(f"[bronze] loaded {n:,} raw rows")
    quality = build_silver(con)
    print(f"[silver] {quality['silver_rows']:,} clean rows "
          f"({quality['rejection_rate_pct']}% rejected)")
    for reason, cnt in quality["rejected"].items():
        print(f"           - {reason}: {cnt}")
    gold = build_gold(con)
    print(f"[gold]   built {len(gold)} tables")

    fc = forecast_revenue(gold["gold_daily_sales"])
    m = fc["metrics"]
    print(f"[ml]     orders R2={m['orders_r2']}  revenue R2={m['revenue_r2']}  "
          f"MAPE={m['revenue_mape_pct']}%")

    s = pd.read_sql(
        "SELECT ROUND(SUM(revenue),2) r, COUNT(*) o, COUNT(DISTINCT customer_id) c, "
        "ROUND(AVG(revenue),2) a FROM silver_orders", con
    ).iloc[0]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "kpis": {
            "revenue": float(s["r"]), "orders": int(s["o"]),
            "customers": int(s["c"]), "avg_order_value": float(s["a"]),
        },
        "data_quality": quality,
        "forecast": fc,
        **{k.replace("gold_", ""): v.to_dict("records") for k, v in gold.items()},
    }
    out_json.write_text(json.dumps(payload, indent=1))
    con.close()
    print(f"[export] {out_json}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=Path("data/raw_orders.csv"))
    ap.add_argument("--db", type=Path, default=Path("data/pulseboard.db"))
    ap.add_argument("--out", type=Path, default=Path("docs/data/gold.json"))
    a = ap.parse_args()
    main(a.csv, a.db, a.out)
