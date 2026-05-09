"""
Creates the BigQuery datasets required by the NYC Taxi ELT pipeline.

Required env vars:
  GOOGLE_APPLICATION_CREDENTIALS  path to GCP service account JSON key
  GCP_PROJECT_ID                   GCP project to create datasets in

Optional env vars:
  GCP_REGION   BigQuery dataset location (default: US)
"""

import os
import sys

from google.api_core.exceptions import Conflict, GoogleAPICallError
from google.cloud import bigquery

DATASETS = {
    "nyc_taxi_raw": "Landing zone for raw NYC Taxi trip data loaded from source.",
    "nyc_taxi_analytics": "Business-ready marts and aggregations for NYC Taxi analysis.",
}


def get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: Required environment variable '{name}' is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def build_client(project_id: str) -> bigquery.Client:
    # The client automatically picks up GOOGLE_APPLICATION_CREDENTIALS from the environment.
    # We validate the var is present before constructing so the error message is clear.
    get_env("GOOGLE_APPLICATION_CREDENTIALS")
    return bigquery.Client(project=project_id)


def create_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    description: str,
    location: str,
) -> None:
    full_id = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(full_id)
    dataset.location = location
    dataset.description = description

    try:
        client.create_dataset(dataset, timeout=30)
        print(f"  [created]  {full_id}  (location: {location})")
    except Conflict:
        print(f"  [exists]   {full_id}  — already exists, skipping.")
    except GoogleAPICallError as exc:
        print(f"  [failed]   {full_id}  — {exc.message}", file=sys.stderr)
        raise


def main() -> None:
    project_id = get_env("GCP_PROJECT_ID")
    location = os.environ.get("GCP_REGION", "US")

    print(f"Project : {project_id}")
    print(f"Location: {location}")
    print(f"Credentials: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}\n")

    try:
        client = build_client(project_id)
    except Exception as exc:
        print(f"ERROR: Failed to create BigQuery client — {exc}", file=sys.stderr)
        sys.exit(1)

    print("Creating datasets...")
    failed = False
    for dataset_id, description in DATASETS.items():
        try:
            create_dataset(client, project_id, dataset_id, description, location)
        except GoogleAPICallError:
            failed = True

    print()
    if failed:
        print("Setup completed with errors. Check output above.", file=sys.stderr)
        sys.exit(1)

    print("Setup complete. All datasets are ready.")


if __name__ == "__main__":
    main()
