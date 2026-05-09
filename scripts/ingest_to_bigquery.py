"""
Downloads NYC Yellow Taxi trip data (Jan 2023) and loads it into BigQuery.

Table: <GCP_PROJECT_ID>.nyc_taxi_raw.yellow_trips_raw
Disposition: WRITE_TRUNCATE (full refresh on each run)

Required env vars:
  GOOGLE_APPLICATION_CREDENTIALS  path to GCP service account JSON key
  GCP_PROJECT_ID                   GCP project containing the dataset

Optional env vars:
  GCP_REGION   BigQuery dataset location (default: US)
"""

import io
import os
import sys

import pandas as pd
import requests
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)
DATASET = "nyc_taxi_raw"
TABLE = "yellow_trips_raw"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB download chunks


def get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: Required environment variable '{name}' is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def download_parquet(url: str) -> pd.DataFrame:
    print(f"Downloading: {url}")
    try:
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))
            buffer = io.BytesIO()
            downloaded = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                buffer.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"  {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB ({pct:.0f}%)", end="\r")
            print()  # newline after progress line
    except requests.exceptions.Timeout:
        print("ERROR: Download timed out after 120 seconds.", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.HTTPError as exc:
        print(f"ERROR: HTTP {exc.response.status_code} when fetching data.", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.RequestException as exc:
        print(f"ERROR: Download failed — {exc}", file=sys.stderr)
        sys.exit(1)

    buffer.seek(0)
    df = pd.read_parquet(buffer)
    print(f"Downloaded {len(df):,} rows, {len(df.columns)} columns.")
    return df


def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    # BigQuery does not accept column names with spaces or special characters.
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Ensure mixed-type object columns are cast to string so pyarrow doesn't fail.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)

    return df


def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    location: str,
) -> int:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    print(f"Loading into BigQuery: {table_ref}")
    print(f"  Write disposition : WRITE_TRUNCATE")
    print(f"  Location          : {location}")

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # block until complete
    except GoogleAPICallError as exc:
        print(f"ERROR: BigQuery load failed — {exc.message}", file=sys.stderr)
        sys.exit(1)

    table_obj = client.get_table(table_ref)
    return table_obj.num_rows


def main() -> None:
    project_id = get_env("GCP_PROJECT_ID")
    get_env("GOOGLE_APPLICATION_CREDENTIALS")  # validate presence; SDK picks it up automatically
    location = os.environ.get("GCP_REGION", "US")

    print(f"Project : {project_id}")
    print(f"Target  : {project_id}.{DATASET}.{TABLE}\n")

    df = download_parquet(SOURCE_URL)
    df = normalize_schema(df)

    row_count = load_to_bigquery(df, project_id, DATASET, TABLE, location)

    print(f"\nSuccess. {row_count:,} rows now in {project_id}.{DATASET}.{TABLE}")


if __name__ == "__main__":
    main()
