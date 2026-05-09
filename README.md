# NYC Taxi Analytics Platform
### Production-Grade ELT Pipeline · Airflow · dbt · BigQuery · Docker · Tableau

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.7-017CEE?logo=apacheairflow)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt)
![BigQuery](https://img.shields.io/badge/BigQuery-Google_Cloud-4285F4?logo=googlecloud)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Overview

End-to-end batch ELT pipeline ingesting NYC Yellow Taxi trip data into BigQuery, orchestrated with Apache Airflow, transformed with dbt, and visualized in Tableau.

**Dataset:** NYC TLC Yellow Taxi Trips — January 2023 (~3M rows)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                    │
│         (Dockerized · LocalExecutor · @daily)           │
└──────────┬──────────────┬──────────────┬────────────────┘
           │              │              │
           ▼              ▼              ▼
    ingest_raw_data   dbt_run       dbt_test
           │              │              │
           ▼              ▼              ▼
┌─────────────────────────────────────────────────────────┐
│                      BigQuery                           │
│                                                         │
│  nyc_taxi_raw          nyc_taxi_analytics               │
│  └─ yellow_trips_raw   ├─ stg_yellow_trips (view)       │
│                        ├─ mart_daily_taxi_summary       │
│                        └─ dim_taxi_zones_scd2           │
└─────────────────────────────────────────────────────────┘
           │
           ▼
      Tableau Public Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.7 (Docker Compose) |
| Transformation | dbt 1.8 + dbt-bigquery |
| Data Warehouse | Google BigQuery (free tier) |
| Ingestion | Python · pandas · google-cloud-bigquery |
| Containerization | Docker Compose |
| Visualization | Tableau Public |

---

## dbt Model Lineage

```
Source: nyc_taxi_raw.yellow_trips_raw
    │
    ▼
staging.stg_yellow_trips (view)
    │   • Renamed columns to snake_case
    │   • Cast timestamps, filtered invalid trips
    │   • Derived trip_duration_minutes
    │
    ▼
marts.mart_daily_taxi_summary (table)
    │   • Aggregated by pickup_date + location
    │   • total_trips, avg_fare, total_revenue
    │   • credit_card_trips, cash_trips split
    │
seeds.taxi_zone_lookup → snapshots.dim_taxi_zones_scd2
        • Type 2 SCD with dbt_valid_from / dbt_valid_to
        • Tracks historical zone/borough changes
```

---

## Pipeline DAG

```
ingest_raw_data >> run_dbt_models >> run_dbt_tests
```

- **ingest_raw_data** — downloads NYC TLC parquet, loads to BigQuery raw layer
- **run_dbt_models** — runs staging + marts transformations
- **run_dbt_tests** — runs 23 data quality tests (not_null, unique, accepted_values, custom)

---

## dbt Test Coverage

| Test Type | Count |
|---|---|
| not_null | 18 |
| accepted_values | 2 |
| Custom (assert_positive_fare) | 1 |
| Source freshness | 2 |
| **Total** | **23** |

---

## Dashboard

📊 [NYC Taxi Analytics — January 2023 on Tableau Public](https://public.tableau.com/app/profile/shivani.rao/viz/NYCTaxiAnalyticsJanuary2023/NYCTaxiAnalyticsJanuary2023?publish=yes)

**Views:**
- Trip demand by pickup zone
- Daily revenue trend (Jan 1–31, 2023)
- Avg fare heatmap by zone × day

---

## Setup

### Prerequisites
- Docker Desktop
- Python 3.9+
- Google Cloud account (free tier)
- dbt-bigquery (`pip install dbt-bigquery`)

### Run locally

```bash
# 1. Clone the repo
git clone https://github.com/Shivanirao2000/airflow-dbt-bq-pipeline
cd airflow-dbt-bq-pipeline

# 2. Set up environment
cp infra/.env.example infra/.env
# Fill in GCP_PROJECT_ID, GCP_KEYFILE_PATH, Fernet key, etc.

# 3. Start Airflow
cd infra
docker-compose up airflow-init
docker-compose up -d

# 4. Run dbt manually
cd ../dbt
export GCP_PROJECT_ID=your-project-id
dbt run
dbt test

# 5. Trigger full pipeline via Airflow UI
# Open http://localhost:8080 → enable nyc_taxi_pipeline DAG → Trigger
```

---

## Project Structure

```
airflow-dbt-bq-pipeline/
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py      # Main Airflow DAG
│   ├── logs/
│   └── plugins/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_yellow_trips.sql  # Staging model
│   │   │   ├── schema.yml            # Column docs + tests
│   │   │   └── sources.yml           # Source definitions
│   │   └── marts/
│   │       └── mart_daily_taxi_summary.sql  # Analytics mart
│   ├── snapshots/
│   │   └── dim_taxi_zones_scd2.sql   # SCD Type 2 snapshot
│   ├── seeds/
│   │   └── taxi_zone_lookup.csv      # Zone reference data
│   ├── macros/
│   │   └── generate_schema_name.sql  # Custom schema macro
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   ├── ingest_to_bigquery.py         # Raw data ingestion
│   └── setup_bigquery.py             # Dataset setup
├── infra/
│   ├── docker-compose.yml            # Airflow stack
│   └── .env.example                  # Environment template
└── README.md
```

---

## Key Results

- Ingested **2,989,774** cleaned trips from 3,066,766 raw records
- **23/23** dbt data quality tests passing
- Final mart table: **6,486 rows** (daily × zone aggregations)
- Weekly revenue pattern visible: ~$2M on Sundays, ~$3M on Fridays
- Type 2 SCD dimension table tracking historical taxi zone changes