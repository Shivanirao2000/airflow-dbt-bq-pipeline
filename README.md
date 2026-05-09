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

## Key Results

- Ingested **2,989,774** cleaned trips from 3,066,766 raw records
- **23/23** dbt data quality tests passing
- Final mart table: **6,486 rows** (daily × zone aggregations)
- Weekly revenue pattern visible: ~$2M on Sundays, ~$3M on Fridays