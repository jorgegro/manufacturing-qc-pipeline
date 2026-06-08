"""
dag_01_ingest_validate.py
─────────────────────────
DAG 1 — Ingest & Validate

Reads raw production CSV for a given date, runs a suite of data-quality
checks, then routes each record to either the "clean" or "quarantine"
bucket. A summary quality report is written alongside the clean output.

Schedule: daily at 07:00 (after the overnight batch lands)
Owner   : jorge.guerrero
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ── Paths (override via Airflow Variables in production) ──────────────────────
BASE_DIR       = Path(os.getenv("PIPELINE_BASE_DIR", "/opt/airflow/data"))
RAW_DIR        = BASE_DIR / "raw"
PROCESSED_DIR  = BASE_DIR / "processed"
QUARANTINE_DIR = BASE_DIR / "quarantine"

# ── Quality rule thresholds ───────────────────────────────────────────────────
RULES = {
    "attenuation_db_km":    (0.18, 0.35),
    "core_diameter_um":     (8.0,  9.5),
    "tensile_strength_mpa": (700,  900),
    "temperature_c":        (18.0, 25.0),
    "draw_speed_m_min":     (800,  1200),
}
REQUIRED_FIELDS = ["record_id", "production_date", "timestamp",
                   "production_line", "product_code", "operator_id"]
NUMERIC_FIELDS  = list(RULES.keys()) + ["throughput_m", "defect_flag"]


# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner":            "jorge.guerrero",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_01_ingest_validate",
    description="Ingest raw production CSV and validate data quality",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["manufacturing", "ingestion", "data-quality"],
)
def ingest_validate_dag():

    @task()
    def ingest(ds: str = None) -> dict:
        """
        Read the raw CSV for the execution date.
        Returns basic stats for downstream tasks.
        """
        date_str = ds or datetime.today().strftime("%Y-%m-%d")
        raw_file = RAW_DIR / f"production_{date_str}.csv"

        if not raw_file.exists():
            raise FileNotFoundError(
                f"Raw file not found: {raw_file}. "
                "Run scripts/generate_data.py first."
            )

        df = pd.read_csv(raw_file, dtype=str)   # read everything as str; validate types next
        logger.info("Ingested %d rows from %s", len(df), raw_file)

        return {
            "date":     date_str,
            "raw_file": str(raw_file),
            "n_rows":   len(df),
        }

    @task()
    def validate(ingest_meta: dict) -> dict:
        """
        Apply data-quality rules and split records into clean / quarantine.

        Rules checked:
          1. Missing required fields (nulls / empty strings)
          2. Duplicate record_ids
          3. Non-numeric values in numeric columns
          4. Out-of-range sensor readings
          5. Negative throughput
        """
        date_str = ingest_meta["date"]
        raw_file = ingest_meta["raw_file"]

        df = pd.read_csv(raw_file, dtype=str)
        df["_row_index"] = df.index
        df["_issues"]    = ""

        # ── Rule 1: required field completeness ───────────────────────────────
        for col in REQUIRED_FIELDS:
            if col in df.columns:
                mask = df[col].isnull() | (df[col].str.strip() == "")
                df.loc[mask, "_issues"] += f"missing_{col}; "

        # ── Rule 2: duplicate record_ids ──────────────────────────────────────
        dup_mask = df.duplicated(subset=["record_id"], keep="first")
        df.loc[dup_mask, "_issues"] += "duplicate_record_id; "

        # ── Rule 3: numeric type coercion ─────────────────────────────────────
        for col in NUMERIC_FIELDS:
            if col in df.columns:
                coerced = pd.to_numeric(df[col], errors="coerce")
                bad     = coerced.isnull() & df[col].notna() & (df[col].str.strip() != "")
                df.loc[bad, "_issues"] += f"non_numeric_{col}; "
                df[col] = coerced

        # ── Rule 4: out-of-range sensor values ───────────────────────────────
        for col, (lo, hi) in RULES.items():
            if col in df.columns:
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                oor = numeric_col.notna() & ((numeric_col < lo) | (numeric_col > hi))
                df.loc[oor, "_issues"] += f"out_of_range_{col}; "

        # ── Rule 5: negative throughput ───────────────────────────────────────
        if "throughput_m" in df.columns:
            neg = pd.to_numeric(df["throughput_m"], errors="coerce") < 0
            df.loc[neg, "_issues"] += "negative_throughput; "

        # ── Split ─────────────────────────────────────────────────────────────
        clean_df     = df[df["_issues"] == ""].copy()
        quarantine_df = df[df["_issues"] != ""].copy()

        # ── Write outputs ─────────────────────────────────────────────────────
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

        clean_file     = PROCESSED_DIR  / f"clean_{date_str}.csv"
        quarantine_file = QUARANTINE_DIR / f"quarantine_{date_str}.csv"

        clean_df.drop(columns=["_row_index", "_issues", "_injected_issue"],
                      errors="ignore").to_csv(clean_file, index=False)
        quarantine_df.to_csv(quarantine_file, index=False)

        n_total      = len(df)
        n_clean      = len(clean_df)
        n_quarantine = len(quarantine_df)
        quality_rate = round(n_clean / n_total * 100, 2) if n_total else 0

        logger.info(
            "Validation complete — total: %d | clean: %d | quarantine: %d | quality rate: %.1f%%",
            n_total, n_clean, n_quarantine, quality_rate,
        )

        return {
            "date":            date_str,
            "clean_file":      str(clean_file),
            "quarantine_file": str(quarantine_file),
            "n_total":         n_total,
            "n_clean":         n_clean,
            "n_quarantine":    n_quarantine,
            "quality_rate":    quality_rate,
        }

    @task()
    def quality_report(validation_meta: dict) -> None:
        """
        Write a JSON quality summary for monitoring / alerting downstream.
        In production this would POST to a Slack webhook or data observability tool.
        """
        report = {
            **validation_meta,
            "generated_at": datetime.utcnow().isoformat(),
            "status":        "PASS" if validation_meta["quality_rate"] >= 85 else "WARN",
        }

        report_file = PROCESSED_DIR / f"quality_report_{validation_meta['date']}.json"
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        report_file.write_text(json.dumps(report, indent=2))

        logger.info("Quality report written → %s", report_file)
        logger.info("Pipeline status: %s (%.1f%% clean)", report["status"], report["quality_rate"])

        if report["status"] == "WARN":
            logger.warning(
                "Quality rate %.1f%% is below 85%% threshold — review quarantine file.",
                report["quality_rate"],
            )

    # ── Wire tasks ────────────────────────────────────────────────────────────
    meta       = ingest()
    val_meta   = validate(meta)
    quality_report(val_meta)


ingest_validate_dag()
