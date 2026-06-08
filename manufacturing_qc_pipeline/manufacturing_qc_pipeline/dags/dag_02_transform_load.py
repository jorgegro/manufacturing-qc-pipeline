"""
dag_02_transform_load.py
─────────────────────────
DAG 2 — Transform & Load

Reads the validated clean CSV produced by DAG 1, applies business-logic
transformations, and loads the result into a warehouse-ready table.

For local / demo runs the "warehouse" is a local SQLite database.
A production version would swap the load_to_warehouse task to target
BigQuery, Snowflake, or PostgreSQL via a connection defined in Airflow.

Schedule: daily at 08:00 (after DAG 1 completes)
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

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(os.getenv("PIPELINE_BASE_DIR", "/opt/airflow/data"))
PROCESSED_DIR = BASE_DIR / "processed"
DB_PATH       = BASE_DIR / "warehouse.db"      # swap for BigQuery dataset in prod

default_args = {
    "owner":            "jorge.guerrero",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_02_transform_load",
    description="Transform clean production data and load to warehouse",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["manufacturing", "transform", "load"],
)
def transform_load_dag():

    @task()
    def read_clean(ds: str = None) -> dict:
        """Load clean CSV written by DAG 1."""
        date_str   = ds or datetime.today().strftime("%Y-%m-%d")
        clean_file = PROCESSED_DIR / f"clean_{date_str}.csv"

        if not clean_file.exists():
            raise FileNotFoundError(
                f"Clean file not found: {clean_file}. "
                "Ensure dag_01_ingest_validate ran successfully first."
            )

        df = pd.read_csv(clean_file)
        logger.info("Loaded %d clean rows for %s", len(df), date_str)
        return {"date": date_str, "n_rows": len(df)}

    @task()
    def transform(read_meta: dict) -> dict:
        """
        Apply business-logic transformations:
          • Cast types
          • Derive quality_grade (A / B / C) based on attenuation
          • Compute shift (day / night) from timestamp
          • Add a pipeline_version watermark for lineage tracking
          • Round floats to 4 dp for consistent downstream storage
        """
        date_str   = read_meta["date"]
        clean_file = PROCESSED_DIR / f"clean_{date_str}.csv"
        df         = pd.read_csv(clean_file)

        # ── Type casting ──────────────────────────────────────────────────────
        df["timestamp"]       = pd.to_datetime(df["timestamp"])
        df["production_date"] = pd.to_datetime(df["production_date"]).dt.date
        for col in ["attenuation_db_km", "core_diameter_um", "tensile_strength_mpa",
                    "temperature_c", "draw_speed_m_min", "throughput_m"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
        df["defect_flag"] = df["defect_flag"].astype(int)

        # ── Derived: quality grade ────────────────────────────────────────────
        # Based on attenuation loss — lower is better
        conditions = [
            df["attenuation_db_km"] <= 0.22,
            df["attenuation_db_km"] <= 0.28,
        ]
        choices    = ["A", "B"]
        df["quality_grade"] = pd.np.select(conditions, choices, default="C") \
            if hasattr(pd, "np") \
            else _quality_grade(df["attenuation_db_km"])

        # ── Derived: production shift ─────────────────────────────────────────
        df["shift"] = df["timestamp"].dt.hour.apply(
            lambda h: "day" if 6 <= h < 18 else "night"
        )

        # ── Watermark ─────────────────────────────────────────────────────────
        df["pipeline_version"]  = "1.0.0"
        df["loaded_at"]         = datetime.utcnow().isoformat()

        # ── Write transformed CSV ─────────────────────────────────────────────
        out_file = PROCESSED_DIR / f"transformed_{date_str}.csv"
        df.to_csv(out_file, index=False)

        logger.info("Transformed %d rows → %s", len(df), out_file)
        return {"date": date_str, "transformed_file": str(out_file), "n_rows": len(df)}

    @task()
    def load_to_warehouse(transform_meta: dict) -> dict:
        """
        Load transformed data into the warehouse table.

        Local demo  → SQLite at data/warehouse.db
        Production  → swap sqlite3 for a BigQuery / Postgres connection hook:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_rows(table="...", rows=df.to_dict("records"))
        """
        date_str         = transform_meta["date"]
        transformed_file = transform_meta["transformed_file"]

        df = pd.read_csv(transformed_file)

        conn = sqlite3.connect(DB_PATH)
        try:
            # Create table on first run, then remove any existing rows for
            # this date before inserting (idempotent load)
            df.head(0).to_sql("production_fact", conn, if_exists="append", index=False)
            conn.execute(
                "DELETE FROM production_fact WHERE production_date = ?", (date_str,)
            )
            df.to_sql("production_fact", conn, if_exists="append", index=False)
            conn.commit()
            logger.info("Loaded %d rows into warehouse for date %s", len(df), date_str)
        finally:
            conn.close()

        return {
            "date":      date_str,
            "rows_loaded": len(df),
            "warehouse": str(DB_PATH),
        }

    @task()
    def summary_report(load_meta: dict) -> None:
        """
        Pull a simple aggregate from the warehouse and log it.
        In production this would write to a BI layer or Slack notification.
        """
        date_str = load_meta["date"]
        conn     = sqlite3.connect(DB_PATH)
        try:
            query = f"""
                SELECT
                    production_line,
                    product_code,
                    COUNT(*)                         AS records,
                    ROUND(AVG(attenuation_db_km), 4) AS avg_attenuation,
                    ROUND(AVG(throughput_m), 0)       AS avg_throughput_m,
                    SUM(defect_flag)                  AS total_defects,
                    quality_grade
                FROM production_fact
                WHERE production_date = '{date_str}'
                GROUP BY production_line, product_code, quality_grade
                ORDER BY production_line, product_code
            """
            df_summary = pd.read_sql(query, conn)
        finally:
            conn.close()

        logger.info("\n%s", df_summary.to_string(index=False))
        logger.info("DAG 2 complete — %d rows loaded for %s", load_meta["rows_loaded"], date_str)

    # ── Wire tasks ────────────────────────────────────────────────────────────
    read_meta      = read_clean()
    transform_meta = transform(read_meta)
    load_meta      = load_to_warehouse(transform_meta)
    summary_report(load_meta)


def _quality_grade(series: pd.Series) -> pd.Series:
    """Fallback for pandas versions without pd.np."""
    import numpy as np
    return np.select(
        [series <= 0.22, series <= 0.28],
        ["A", "B"],
        default="C",
    )


transform_load_dag()
