"""Pipeline tests: run with `pytest -q` from the repo root."""
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))

import generate_data  # noqa: E402
import medallion  # noqa: E402


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    d = tmp_path_factory.mktemp("pb")
    csv, db, js = d / "raw.csv", d / "t.db", d / "gold.json"
    generate_data.main(rows=4000, out=csv, seed=7)
    medallion.main(csv, db, js)
    con = sqlite3.connect(db)
    yield con, js
    con.close()


def test_silver_has_no_invalid_rows(built):
    con, _ = built
    s = pd.read_sql("SELECT * FROM silver_orders", con)
    assert (s["quantity"] > 0).all()
    assert (s["unit_price"] > 0).all()
    assert s["order_id"].is_unique
    assert s["order_ts"].notna().all()


def test_silver_text_is_standardised(built):
    con, _ = built
    s = pd.read_sql("SELECT region, product FROM silver_orders", con)
    assert set(s["region"]) <= {"North", "South", "East", "West"}
    assert (s["product"] == s["product"].str.title()).all()


def test_row_counts_reconcile(built):
    """bronze = silver + every rejection bucket (nothing silently lost)."""
    con, _ = built
    b = pd.read_sql("SELECT COUNT(*) n FROM bronze_orders", con).n[0]
    s = pd.read_sql("SELECT COUNT(*) n FROM silver_orders", con).n[0]
    import json
    rejected = sum(json.loads(built[1].read_text())["data_quality"]["rejected"].values())
    assert b == s + rejected


def test_gold_revenue_matches_silver(built):
    con, _ = built
    silver = pd.read_sql("SELECT SUM(revenue) r FROM silver_orders", con).r[0]
    for table in ["gold_daily_sales", "gold_product_summary", "gold_region_summary"]:
        gold = pd.read_sql(f"SELECT SUM(revenue) r FROM {table}", con).r[0]
        assert gold == pytest.approx(silver, rel=1e-6), table


def test_forecast_shape_and_bounds(built):
    import json
    fc = json.loads(built[1].read_text())["forecast"]["forecast"]
    assert len(fc) == 30
    assert all(r["lower"] <= r["revenue"] <= r["upper"] for r in fc)
    assert all(r["revenue"] >= 0 for r in fc)
