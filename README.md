# Airflow + dbt + BigQuery ELT Pipeline

An end-to-end ELT (Extract, Load, Transform) pipeline using Apache Airflow for orchestration, dbt for transformation, and Google BigQuery as the data warehouse.

## Architecture

```
Source Data
    │
    ▼
scripts/ingest.py          ← Loads raw data into BigQuery (raw dataset)
    │
    ▼
BigQuery: raw dataset      ← Landing zone for raw, untransformed data
    │
    ▼
dbt staging models         ← Light cleaning, renaming, type casting (views)
    │
    ▼
dbt marts models           ← Business-ready aggregations and joins (tables)
    │
    ▼
BigQuery: marts dataset    ← Consumed by BI tools / downstream consumers
```

Airflow orchestrates the full sequence: ingest → dbt run → dbt test.

## Project Structure

```
airflow-dbt-bq-pipeline/
├── airflow/
│   ├── dags/                   # Airflow DAG definitions
│   │   └── elt_pipeline_dag.py # Main ELT orchestration DAG
│   ├── plugins/                # Custom Airflow operators/hooks
│   └── logs/                   # Airflow task logs (git-ignored)
│
├── dbt/
│   ├── models/
│   │   ├── staging/            # Staging models (views over raw tables)
│   │   │   ├── sources.yml     # Source table declarations + freshness tests
│   │   │   └── stg_example.sql
│   │   └── marts/              # Business-layer models (materialized tables)
│   │       └── mart_example.sql
│   ├── tests/                  # Custom singular dbt tests
│   ├── macros/                 # Reusable Jinja macros
│   │   └── generate_schema_name.sql
│   ├── seeds/                  # Static CSV reference data
│   ├── dbt_project.yml         # dbt project configuration
│   └── profiles.yml            # BigQuery connection profiles (dev + prod)
│
├── infra/
│   ├── docker-compose.yml      # Airflow + Postgres stack for local development
│   └── .env.example            # Environment variable template
│
├── scripts/
│   └── ingest.py               # Data ingestion script (source → BQ raw)
│
├── .gitignore
└── README.md
```

## Prerequisites

- Docker and Docker Compose
- Google Cloud project with BigQuery enabled
- A GCP service account key with BigQuery Data Editor + Job User roles
- Python 3.11+

## Quickstart

**1. Clone and configure environment**

```bash
cp infra/.env.example .env
# Edit .env with your GCP project ID and keyfile path
```

**2. Create BigQuery datasets**

```bash
bq mk --dataset ${GCP_PROJECT_ID}:raw
bq mk --dataset ${GCP_PROJECT_ID}:staging
bq mk --dataset ${GCP_PROJECT_ID}:marts
```

**3. Start Airflow**

```bash
docker compose -f infra/docker-compose.yml up airflow-init
docker compose -f infra/docker-compose.yml up -d
```

Airflow UI → http://localhost:8080 (admin / admin)

**4. Install dbt dependencies (local dev)**

```bash
pip install dbt-bigquery
cd dbt && dbt deps
```

**5. Validate dbt connection**

```bash
cd dbt && dbt debug --profiles-dir . --target dev
```

**6. Trigger the pipeline**

Enable and trigger `elt_pipeline` in the Airflow UI, or run manually:

```bash
airflow dags trigger elt_pipeline
```

## DAG Overview

`elt_pipeline` runs daily with three sequential tasks:

| Task | What it does |
|------|-------------|
| `ingest_raw_data` | Runs `scripts/ingest.py` to load source data into `raw` dataset |
| `dbt_run` | Executes all dbt models (staging views → mart tables) |
| `dbt_test` | Runs dbt data quality tests |

## dbt Model Layers

| Layer | Location | Materialization | Purpose |
|-------|----------|----------------|---------|
| Staging | `models/staging/` | View | Rename, cast, deduplicate raw data |
| Marts | `models/marts/` | Table | Business aggregations for BI consumption |

## Adding a New Data Source

1. Add the source table definition to `dbt/models/staging/sources.yml`
2. Create a staging model `dbt/models/staging/stg_<source>.sql`
3. Create mart models in `dbt/models/marts/` that `ref()` the staging model
4. Add ingestion logic to `scripts/ingest.py`
5. Update the Airflow DAG if the ingestion step changes

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Your Google Cloud project ID |
| `GCP_KEYFILE_PATH` | Absolute path to your GCP service account JSON key |
| `BQ_DATASET_RAW` | BigQuery dataset for raw landing data (default: `raw`) |
| `BQ_DATASET_STAGING` | BigQuery dataset for staging models (default: `staging`) |
| `BQ_DATASET_MARTS` | BigQuery dataset for mart models (default: `marts`) |
| `AIRFLOW__CORE__FERNET_KEY` | Fernet key for Airflow secret encryption |
