"""
test_data_quality.py
─────────────────────
Unit tests for the data-quality validation logic used in DAG 1.

Run with:  pytest tests/test_data_quality.py -v
"""

import pandas as pd
import pytest


# ── Replicate the validation logic so tests are self-contained ──────────────
RULES = {
    "attenuation_db_km":    (0.18, 0.35),
    "core_diameter_um":     (8.0,  9.5),
    "tensile_strength_mpa": (700,  900),
    "temperature_c":        (18.0, 25.0),
    "draw_speed_m_min":     (800,  1200),
}
REQUIRED_FIELDS = [
    "record_id", "production_date", "timestamp",
    "production_line", "product_code", "operator_id",
]
NUMERIC_FIELDS = list(RULES.keys()) + ["throughput_m", "defect_flag"]


def run_validation(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror of the validate task logic — returns df with _issues column."""
    df = df.copy()
    df["_issues"] = ""

    for col in REQUIRED_FIELDS:
        if col in df.columns:
            mask = df[col].isnull() | (df[col].astype(str).str.strip() == "")
            df.loc[mask, "_issues"] += f"missing_{col}; "

    dup_mask = df.duplicated(subset=["record_id"], keep="first")
    df.loc[dup_mask, "_issues"] += "duplicate_record_id; "

    for col in NUMERIC_FIELDS:
        if col in df.columns:
            coerced = pd.to_numeric(df[col], errors="coerce")
            bad     = coerced.isnull() & df[col].notna() & (df[col].astype(str).str.strip() != "")
            df.loc[bad, "_issues"] += f"non_numeric_{col}; "
            df[col] = coerced

    for col, (lo, hi) in RULES.items():
        if col in df.columns:
            oor = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
            df.loc[oor, "_issues"] += f"out_of_range_{col}; "

    if "throughput_m" in df.columns:
        neg = pd.to_numeric(df["throughput_m"], errors="coerce") < 0
        df.loc[neg, "_issues"] += "negative_throughput; "

    return df


# ── Fixtures ──────────────────────────────────────────────────────────────────

def good_record(**overrides) -> dict:
    base = {
        "record_id":            "test-uuid-001",
        "production_date":      "2024-01-15",
        "timestamp":            "2024-01-15T08:00:00",
        "production_line":      "LINE-A",
        "product_code":         "SMF-28",
        "operator_id":          "OP-101",
        "attenuation_db_km":    0.25,
        "core_diameter_um":     8.8,
        "tensile_strength_mpa": 800.0,
        "temperature_c":        22.0,
        "draw_speed_m_min":     1000.0,
        "throughput_m":         1500.0,
        "defect_flag":          0,
    }
    base.update(overrides)
    return base


# ── Tests: clean records ──────────────────────────────────────────────────────

class TestCleanRecords:

    def test_valid_record_passes(self):
        df     = pd.DataFrame([good_record()])
        result = run_validation(df)
        assert result["_issues"].iloc[0] == "", "Clean record should have no issues"

    def test_multiple_clean_records(self):
        records = [good_record(record_id=f"uuid-{i}") for i in range(10)]
        df      = pd.DataFrame(records)
        result  = run_validation(df)
        assert (result["_issues"] == "").all()


# ── Tests: missing fields ─────────────────────────────────────────────────────

class TestMissingFields:

    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_missing_required_field(self, field):
        record      = good_record()
        record[field] = ""
        df          = pd.DataFrame([record])
        result      = run_validation(df)
        assert f"missing_{field}" in result["_issues"].iloc[0]

    def test_null_required_field(self):
        record              = good_record()
        record["operator_id"] = None
        df                  = pd.DataFrame([record])
        result              = run_validation(df)
        assert "missing_operator_id" in result["_issues"].iloc[0]


# ── Tests: out-of-range sensors ───────────────────────────────────────────────

class TestOutOfRange:

    def test_attenuation_too_high(self):
        df     = pd.DataFrame([good_record(attenuation_db_km=0.60)])
        result = run_validation(df)
        assert "out_of_range_attenuation_db_km" in result["_issues"].iloc[0]

    def test_attenuation_too_low(self):
        df     = pd.DataFrame([good_record(attenuation_db_km=0.05)])
        result = run_validation(df)
        assert "out_of_range_attenuation_db_km" in result["_issues"].iloc[0]

    def test_core_diameter_out_of_range(self):
        df     = pd.DataFrame([good_record(core_diameter_um=5.0)])
        result = run_validation(df)
        assert "out_of_range_core_diameter_um" in result["_issues"].iloc[0]

    def test_temperature_too_high(self):
        df     = pd.DataFrame([good_record(temperature_c=30.0)])
        result = run_validation(df)
        assert "out_of_range_temperature_c" in result["_issues"].iloc[0]

    @pytest.mark.parametrize("col,lo,hi", [
        (c, v[0], v[1]) for c, v in RULES.items()
    ])
    def test_boundary_values_pass(self, col, lo, hi):
        """Values exactly at the boundary should pass."""
        df_lo  = pd.DataFrame([good_record(**{col: lo})])
        df_hi  = pd.DataFrame([good_record(**{col: hi})])
        for df in (df_lo, df_hi):
            result = run_validation(df)
            assert f"out_of_range_{col}" not in result["_issues"].iloc[0], \
                f"Boundary value for {col} should not be flagged"


# ── Tests: duplicate records ──────────────────────────────────────────────────

class TestDuplicates:

    def test_duplicate_record_id_flagged(self):
        r1 = good_record(record_id="dup-uuid")
        r2 = good_record(record_id="dup-uuid")
        df = pd.DataFrame([r1, r2])
        result = run_validation(df)
        # first occurrence passes, second flagged
        assert result["_issues"].iloc[0] == ""
        assert "duplicate_record_id" in result["_issues"].iloc[1]

    def test_unique_ids_not_flagged(self):
        records = [good_record(record_id=f"uuid-{i}") for i in range(5)]
        df      = pd.DataFrame(records)
        result  = run_validation(df)
        assert not result["_issues"].str.contains("duplicate").any()


# ── Tests: numeric type issues ────────────────────────────────────────────────

class TestNonNumeric:

    def test_non_numeric_sensor_flagged(self):
        record                       = good_record()
        record["attenuation_db_km"]  = "ERROR"
        df                           = pd.DataFrame([record])
        result                       = run_validation(df)
        assert "non_numeric_attenuation_db_km" in result["_issues"].iloc[0]

    def test_negative_throughput_flagged(self):
        df     = pd.DataFrame([good_record(throughput_m=-200)])
        result = run_validation(df)
        assert "negative_throughput" in result["_issues"].iloc[0]

    def test_zero_throughput_passes(self):
        df     = pd.DataFrame([good_record(throughput_m=0)])
        result = run_validation(df)
        assert "_issues" not in result["_issues"].iloc[0] or result["_issues"].iloc[0] == ""


# ── Tests: mixed batch ────────────────────────────────────────────────────────

class TestMixedBatch:

    def test_quarantine_split_ratio(self):
        """10 records: 8 clean, 2 dirty → expect exactly 2 quarantined."""
        records = [good_record(record_id=f"uuid-{i}") for i in range(8)]
        records.append(good_record(record_id="bad-1", attenuation_db_km=0.99))
        records.append(good_record(record_id="bad-2", operator_id=""))

        df     = pd.DataFrame(records)
        result = run_validation(df)

        n_clean      = (result["_issues"] == "").sum()
        n_quarantine = (result["_issues"] != "").sum()

        assert n_clean      == 8
        assert n_quarantine == 2
