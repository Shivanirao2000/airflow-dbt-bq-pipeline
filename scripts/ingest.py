import os
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_RAW = os.environ.get("BQ_DATASET_RAW", "raw")


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def load_data_to_bq(client: bigquery.Client, table_id: str, rows: list[dict]) -> None:
    errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_RAW}.{table_id}", rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    print(f"Loaded {len(rows)} rows into {DATASET_RAW}.{table_id}")


def ingest_example() -> None:
    client = get_bq_client()

    # Replace with your actual data source logic
    rows = [
        {"id": 1, "name": "example_a", "created_at": "2024-01-01", "updated_at": "2024-01-01"},
        {"id": 2, "name": "example_b", "created_at": "2024-01-02", "updated_at": "2024-01-02"},
    ]

    load_data_to_bq(client, "example_table", rows)


if __name__ == "__main__":
    ingest_example()
