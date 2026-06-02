# 🏭 Manufacturing QC Pipeline

A production-grade Apache Airflow project that simulates a **fiber optic cable manufacturing** data pipeline — from raw sensor ingestion through data quality validation, warehouse loading, and ML feature engineering.

Built to demonstrate core Data Engineering skills: pipeline orchestration, automated data quality, ETL/ELT patterns, and AI/ML data readiness in a manufacturing context.

---

## Architecture

```
Raw Sensor CSV
     │
     ▼
┌─────────────────────────────┐
│  DAG 1 — Ingest & Validate  │  ← Automated quality checks
│  (runs daily @ 07:00)       │    Null checks, range validation,
└──────────────┬──────────────┘    duplicate detection
               │
        ┌──────┴──────┐
        ▼             ▼
   clean CSV     quarantine CSV  ← Bad records isolated for review
        │
        ▼
┌─────────────────────────────┐
│  DAG 2 — Transform & Load   │  ← Type casting, business logic,
│  (runs daily @ 08:00)       │    quality grading, warehouse load
└──────────────┬──────────────┘
               │
               ▼
         warehouse.db  (SQLite locally / BigQuery in prod)
               │
               ▼
┌─────────────────────────────┐
│  DAG 3 — Feature Engineering│  ← Lag features, rolling stats,
│  (runs daily @ 09:00)       │    categorical encoding → ML-ready
└──────────────┬──────────────┘
               │
               ▼
         features_{date}.csv   ← Handed off to ML model training
```

---

## Project Structure

```
manufacturing_qc_pipeline/
├── dags/
│   ├── dag_01_ingest_validate.py    # Ingest raw CSV + quality checks
│   ├── dag_02_transform_load.py     # Transform + load to warehouse
│   └── dag_03_feature_engineering.py # ML feature table
├── scripts/
│   └── generate_data.py             # Synthetic data generator
├── data/
│   ├── raw/                         # Landing zone for raw CSVs
│   ├── processed/                   # Clean data + quality reports
│   ├── quarantine/                  # Flagged/rejected records
│   └── features/                    # ML-ready feature tables
├── tests/
│   └── test_data_quality.py         # Unit tests for quality rules
├── docker-compose.yml               # Local Airflow environment
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### 1. Clone & set up

```bash
git clone https://github.com/jorgegro/manufacturing-qc-pipeline
cd manufacturing-qc-pipeline

# Create data directories
mkdir -p data/{raw,processed,quarantine,features}
```

### 2. Start Airflow

```bash
docker compose up -d

# Wait ~30 seconds for init, then open:
# http://localhost:8080  (airflow / airflow)
```

### 3. Generate sample data

```bash
# Generate 500 sensor readings for today (with ~10% dirty records injected)
python scripts/generate_data.py

# Or for a specific date
python scripts/generate_data.py --date 2024-01-15 --records 1000
```

### 4. Run the pipeline

In the Airflow UI:
1. Enable `dag_01_ingest_validate` → trigger manually
2. After it succeeds, enable and trigger `dag_02_transform_load`
3. After it succeeds, enable and trigger `dag_03_feature_engineering`

Or trigger via CLI:
```bash
docker compose exec airflow-webserver airflow dags trigger dag_01_ingest_validate
```

### 5. Run tests

```bash
pip install pytest pandas
pytest tests/ -v
```

---

## Data Quality Rules (DAG 1)

| Rule | Description |
|------|-------------|
| Required fields | `record_id`, `production_date`, `timestamp`, `production_line`, `product_code`, `operator_id` must be non-null |
| Duplicate detection | Flags records with duplicate `record_id` (keeps first) |
| Numeric validation | Ensures sensor fields are parseable as float |
| Range checks | Attenuation: 0.18–0.35 dB/km, Core: 8.0–9.5 µm, Temp: 18–25 °C, etc. |
| Negative throughput | Flags any negative production meter readings |

Records failing any rule are routed to `data/quarantine/quarantine_{date}.csv` with the specific issue(s) noted. A quality report JSON is emitted for monitoring.

---

## ML Feature Table (DAG 3)

The feature engineering DAG produces a model-ready dataset with:

- **Lag features** — Previous sensor reading per production line
- **Rolling statistics** — 10-record rolling mean & std for key sensors
- **Trend indicator** — Is attenuation trending upward (potential degradation)?
- **One-hot encoding** — Line, product type, shift, quality grade
- **Target label** — `defect_flag` (0 = no defect, 1 = defect detected)

This table is ready to be consumed by a scikit-learn or XGBoost defect prediction model without additional preprocessing.

---

## Production Swap-Outs

| Component | Local (demo) | Production |
|-----------|-------------|------------|
| Warehouse | SQLite (`warehouse.db`) | BigQuery / Snowflake / PostgreSQL |
| Scheduler | LocalExecutor | CeleryExecutor / KubernetesExecutor |
| Data source | Generated CSV | Pi Integrator / Kafka / S3 |
| Alerting | Logger | Slack webhook / PagerDuty |
| Quality framework | Custom rules | Great Expectations |

---

## Author

**Jorge André Guerrero Barragán** — [jorgegro1999@gmail.com](mailto:jorgegro1999@gmail.com)

Computer Engineer | Data Engineer | Bilingual EN/ES
