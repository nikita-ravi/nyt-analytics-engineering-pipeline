"""
NYT Analytics Engineering — BigQuery Data Loader
==================================================
Loads all raw Parquet/CSV files directly into BigQuery using the
google-cloud-bigquery Python client. No gcloud CLI required.

Usage:
    python ingestion/load_to_bigquery.py

Datasets created:
    nyt_analytics_engineering.raw_subscribers
    nyt_analytics_engineering.raw_events_news
    nyt_analytics_engineering.raw_events_games
    nyt_analytics_engineering.raw_events_cooking
    nyt_analytics_engineering.raw_events_athletic
"""

import os
import sys
from datetime import datetime

import pandas as pd
from google.cloud import bigquery


PROJECT_ID  = "nyt-analytics-engineering"
DATASET_ID  = "raw_data"
LOCATION    = "US"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")

TABLES = {
    "raw_subscribers":    {"path": os.path.join(DATA_DIR, "subscribers", "subscribers.csv"),   "fmt": "csv"},
    "raw_events_news":    {"path": os.path.join(DATA_DIR, "news",        "events.parquet"),     "fmt": "parquet"},
    "raw_events_games":   {"path": os.path.join(DATA_DIR, "games",       "events.parquet"),     "fmt": "parquet"},
    "raw_events_cooking": {"path": os.path.join(DATA_DIR, "cooking",     "events.parquet"),     "fmt": "parquet"},
    "raw_events_athletic":{"path": os.path.join(DATA_DIR, "athletic",    "events.parquet"),     "fmt": "parquet"},
}

def load_table(client: bigquery.Client, table_name: str, path: str, fmt: str):
    print(f"\n  Loading {table_name}...")
    
    if fmt == "csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)

    # BigQuery doesn't like boolean columns loaded as object — fix
    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].astype(bool)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite on rerun
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # wait for completion

    table = client.get_table(table_ref)
    print(f"  ✅ {table_name}: {table.num_rows:,} rows → {PROJECT_ID}.{DATASET_ID}.{table_name}")
    return table.num_rows


def main():
    print("=" * 60)
    print("NYT Analytics Engineering — BigQuery Loader")
    print(f"Project: {PROJECT_ID}   Dataset: {DATASET_ID}")
    print(f"Run at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Initialize BigQuery client (uses Application Default Credentials)
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"\n✅ Authenticated as: {client.project}")
    except Exception as e:
        print(f"\n❌ Auth failed: {e}")
        print("\nRun this in your terminal first:")
        print("  gcloud auth application-default login")
        print("Or set GOOGLE_APPLICATION_CREDENTIALS to a service account key file.")
        sys.exit(1)

    # Create dataset if needed
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = LOCATION
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset ready: {PROJECT_ID}.{DATASET_ID}")
    except Exception as e:
        print(f"❌ Could not create dataset: {e}")
        sys.exit(1)

    # Load all tables
    total_rows = 0
    for table_name, cfg in TABLES.items():
        if not os.path.exists(cfg["path"]):
            print(f"  ⚠️  File not found: {cfg['path']} — skipping")
            continue
        rows = load_table(client, table_name, cfg["path"], cfg["fmt"])
        total_rows += rows

    print(f"\n\n{'='*60}")
    print(f"✅ Load complete! {total_rows:,} total rows loaded to BigQuery.")
    print(f"\nQuery in BigQuery console:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
    print(f"\nOr run dbt:")
    print(f"  cd dbt_project && dbt run --profiles-dir .")


if __name__ == "__main__":
    main()
