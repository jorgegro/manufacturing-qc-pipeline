"""
dag_03_feature_engineering.py
──────────────────────────────
DAG 3 — Feature Engineering (AI/ML readiness)

Reads the warehouse-loaded production data and produces a clean,
model-ready feature table for a defect-prediction ML model.

This demonstrates the pipeline → ML handoff that the job posting
explicitly calls out under "Support AI/ML data integration."

Feature table schema
────────────────────
Each row = one production reading, enriched with:
  • Rolling 10-record stats per line (avg, std, trend)
  • Lag features (previous reading's key metrics)
  • Categorical encodings (line, product, shift, grade)
  • Target label: defect_flag (binary 0/1)

Schedule: daily at 09:00 (after DAG 2 completes)
Owner   : jorge.guerrero
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

BASE_DIR      = Path(os.getenv("PIPELINE_BASE_DIR", "/opt/airflow/data"))
FEATURES_DIR  = BASE_DIR / "features"
DB_PATH       = BASE_DIR / "warehouse.db"

NUMERIC_SENSORS = [
    "attenuation_db_km", "core_diameter_um", "tensile_strength_mpa",
    "temperature_c", "draw_speed_m_min", "throughput_m",
]

default_args = {
    "owner":            "jorge.guerrero",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_03_feature_engineering",
    description="Build ML-ready feature table for defect prediction model",
    schedule_interval="0 9 * * *",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["manufacturing", "ml", "features", "ai"],
)
def feature_engineering_dag():

    @task()
    def load_from_warehouse(ds: str = None) -> dict:
        """Pull today's production data from the warehouse."""
        date_str = ds or datetime.today().strftime("%Y-%m-%d")

        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql(
                f"SELECT * FROM production_fact WHERE production_date = '{date_str}'",
                conn,
            )
        finally:
            conn.close()

        if df.empty:
            raise ValueError(
                f"No warehouse data found for {date_str}. "
                "Ensure dag_02_transform_load ran successfully."
            )

        logger.info("Loaded %d rows from warehouse for %s", len(df), date_str)
        return {"date": date_str, "n_rows": len(df)}

    @task()
    def engineer_features(load_meta: dict) -> dict:
        """
        Build the feature matrix:

          1. Lag features  — previous sensor reading per line
          2. Rolling stats — 10-record rolling mean + std per line
          3. Trend flag    — is attenuation trending up (degrading)?
          4. Categorical   — one-hot encode line, product, shift, grade
          5. Drop raw cols not useful for the model
        """
        date_str = load_meta["date"]

        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql(
                f"SELECT * FROM production_fact WHERE production_date = '{date_str}'",
                conn,
            )
        finally:
            conn.close()

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(["production_line", "timestamp"]).reset_index(drop=True)

        # ── 1. Lag features (per production line) ─────────────────────────────
        for col in NUMERIC_SENSORS:
            df[f"lag1_{col}"] = df.groupby("production_line")[col].shift(1)

        # ── 2. Rolling statistics (window = 10) ───────────────────────────────
        for col in ["attenuation_db_km", "temperature_c", "draw_speed_m_min"]:
            grp = df.groupby("production_line")[col]
            df[f"roll10_mean_{col}"] = grp.transform(lambda x: x.rolling(10, min_periods=1).mean()).round(4)
            df[f"roll10_std_{col}"]  = grp.transform(lambda x: x.rolling(10, min_periods=1).std()).round(4)

        # ── 3. Trend flag — attenuation rising over last 5 readings? ─────────
        df["attenuation_trend_up"] = (
            df.groupby("production_line")["attenuation_db_km"]
            .transform(lambda x: (x.diff(5) > 0).astype(int))
        )

        # ── 4. Categorical encoding ───────────────────────────────────────────
        df = pd.get_dummies(
            df,
            columns=["production_line", "product_code", "shift", "quality_grade"],
            prefix=["line", "product", "shift", "grade"],
            drop_first=False,
        )

        # ── 5. Drop columns not suitable as model features ────────────────────
        drop_cols = [
            "record_id", "production_date", "timestamp", "operator_id",
            "pipeline_version", "loaded_at", "_injected_issue",
        ]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        # ── Fill NaN from lag/rolling at window boundaries ────────────────────
        df = df.fillna(0)

        # ── Write feature table ───────────────────────────────────────────────
        FEATURES_DIR.mkdir(parents=True, exist_ok=True)
        feature_file = FEATURES_DIR / f"features_{date_str}.csv"
        df.to_csv(feature_file, index=False)

        n_features = len([c for c in df.columns if c != "defect_flag"])
        logger.info(
            "Feature table written → %s | rows: %d | features: %d",
            feature_file, len(df), n_features,
        )
        return {
            "date":         date_str,
            "feature_file": str(feature_file),
            "n_rows":       len(df),
            "n_features":   n_features,
        }

    @task()
    def validate_features(feature_meta: dict) -> None:
        """
        Sanity-check the feature table before handing off to the model layer.

        Checks:
          • No column is 100% null
          • Target label (defect_flag) exists and is binary
          • Feature count is within expected range
          • No inf values
        """
        import numpy as np

        feature_file = feature_meta["feature_file"]
        df           = pd.read_csv(feature_file)

        issues = []

        # All-null columns
        null_cols = [c for c in df.columns if df[c].isnull().all()]
        if null_cols:
            issues.append(f"All-null columns: {null_cols}")

        # Target label
        if "defect_flag" not in df.columns:
            issues.append("Target column 'defect_flag' is missing")
        else:
            unique_vals = set(df["defect_flag"].unique())
            if not unique_vals.issubset({0, 1}):
                issues.append(f"defect_flag has unexpected values: {unique_vals}")

        # Feature count
        n_features = feature_meta["n_features"]
        if n_features < 10:
            issues.append(f"Suspiciously few features: {n_features}")

        # Inf values
        numeric_df = df.select_dtypes(include=[np.number])
        inf_cols   = [c for c in numeric_df.columns if np.isinf(numeric_df[c]).any()]
        if inf_cols:
            issues.append(f"Infinite values in: {inf_cols}")

        if issues:
            for issue in issues:
                logger.error("Feature validation FAILED: %s", issue)
            raise ValueError(f"Feature validation failed with {len(issues)} issue(s). See logs.")

        logger.info(
            "Feature validation PASSED — %d rows, %d features, target label OK",
            feature_meta["n_rows"], n_features,
        )

    # ── Wire tasks ────────────────────────────────────────────────────────────
    load_meta    = load_from_warehouse()
    feature_meta = engineer_features(load_meta)
    validate_features(feature_meta)


feature_engineering_dag()
