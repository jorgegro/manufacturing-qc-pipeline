"""
generate_data.py
Simulates a day's worth of fiber optic cable production sensor readings.
Intentionally injects ~10% dirty records (nulls, out-of-range values, duplicates)
so the quality pipeline has real work to do.

Usage:
    python scripts/generate_data.py --date 2024-01-15 --records 500
    python scripts/generate_data.py  # defaults to today, 500 records
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

# ── Constants ────────────────────────────────────────────────────────────────

LINES = ["LINE-A", "LINE-B", "LINE-C"]
PRODUCTS = ["SMF-28", "OM4-50", "OM3-50", "DSF-1550"]
OPERATORS = ["OP-101", "OP-102", "OP-103", "OP-104"]

# Normal operating ranges (spec limits)
SPEC = {
    "attenuation_db_km":    (0.18, 0.35),   # dB/km — fiber signal loss
    "core_diameter_um":     (8.0,  9.5),     # µm — single-mode fiber core
    "tensile_strength_mpa": (700,  900),     # MPa — cable pull strength
    "temperature_c":        (18.0, 25.0),    # °C  — draw tower temp
    "draw_speed_m_min":     (800,  1200),    # m/min
    "throughput_m":         (0,    5000),    # metres produced this reading
}

DIRTY_RATE = 0.10  # 10 % of records will be "dirty"


# ── Helpers ──────────────────────────────────────────────────────────────────

def normal_record(ts: datetime, date_str: str) -> dict:
    return {
        "record_id":            str(uuid.uuid4()),
        "production_date":      date_str,
        "timestamp":            ts.isoformat(timespec="seconds"),
        "production_line":      random.choice(LINES),
        "product_code":         random.choice(PRODUCTS),
        "operator_id":          random.choice(OPERATORS),
        "attenuation_db_km":    round(random.uniform(*SPEC["attenuation_db_km"]), 4),
        "core_diameter_um":     round(random.uniform(*SPEC["core_diameter_um"]), 3),
        "tensile_strength_mpa": round(random.uniform(*SPEC["tensile_strength_mpa"]), 1),
        "temperature_c":        round(random.uniform(*SPEC["temperature_c"]), 2),
        "draw_speed_m_min":     round(random.uniform(*SPEC["draw_speed_m_min"]), 1),
        "throughput_m":         round(random.uniform(200, 5000), 0),
        "defect_flag":          random.choices([0, 1], weights=[95, 5])[0],
    }


def dirty_record(base: dict) -> dict:
    """Inject one random quality issue into a record."""
    issue = random.choice([
        "null_sensor",
        "out_of_range_high",
        "out_of_range_low",
        "missing_operator",
        "negative_throughput",
        "null_product",
    ])
    record = base.copy()
    record["_injected_issue"] = issue          # for test validation only

    if issue == "null_sensor":
        field = random.choice(["attenuation_db_km", "core_diameter_um", "temperature_c"])
        record[field] = ""

    elif issue == "out_of_range_high":
        record["attenuation_db_km"] = round(random.uniform(0.50, 0.80), 4)

    elif issue == "out_of_range_low":
        record["core_diameter_um"] = round(random.uniform(4.0, 7.9), 3)

    elif issue == "missing_operator":
        record["operator_id"] = ""

    elif issue == "negative_throughput":
        record["throughput_m"] = round(random.uniform(-500, -1), 0)

    elif issue == "null_product":
        record["product_code"] = ""

    return record


# ── Main ─────────────────────────────────────────────────────────────────────

def generate(date_str: str, n: int, output_dir: str) -> str:
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_time  = target_date.replace(hour=6, minute=0, second=0)

    records = []
    step_seconds = int(8 * 3600 / n)   # spread readings across an 8-hour shift

    for i in range(n):
        ts     = start_time + timedelta(seconds=i * step_seconds)
        record = normal_record(ts, date_str)

        if random.random() < DIRTY_RATE:
            record = dirty_record(record)

        records.append(record)

    # Add a handful of exact duplicates (same record_id)
    for _ in range(max(1, n // 50)):
        records.append(random.choice(records).copy())

    random.shuffle(records)

    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"production_{date_str}.csv")

    fieldnames = [
        "record_id", "production_date", "timestamp", "production_line",
        "product_code", "operator_id", "attenuation_db_km", "core_diameter_um",
        "tensile_strength_mpa", "temperature_c", "draw_speed_m_min",
        "throughput_m", "defect_flag", "_injected_issue",
    ]

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[generate_data] Wrote {len(records)} records → {filename}")
    return filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic manufacturing data")
    parser.add_argument("--date",    default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--records", type=int, default=500)
    parser.add_argument("--output",  default="data/raw")
    args = parser.parse_args()

    generate(args.date, args.records, args.output)
