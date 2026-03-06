"""
NYT Analytics Engineering — Late Arrival Detector
===================================================
Pain point: Mobile events don't always land on time.
A subscriber plays Wordle at 11 PM Tuesday (airplane mode). Their phone
reconnects at 7 AM Wednesday. The 6 AM pipeline already ran. Tuesday's
DAU is permanently understated — that subscriber gets a false churn alert.

This detector:
  1. Simulates "file arrival timestamps" (when GCS received the file)
     vs "event timestamps" (when the event actually happened)
  2. Measures lag distribution per product domain
  3. Identifies records requiring reprocessing (lag > threshold)
  4. Produces a reprocessing manifest consumed by the Airflow DAG's
     second task group (reprocess_late_arrivals)

Real production equivalent:
  - GCS object metadata has a `timeCreated` field
  - Compare event.event_timestamp vs object.timeCreated
  - Any event where (arrival - event) > 2 hours is "late"
  - Airflow reprocesses the prior 48 hours of staging each morning

Output:
  data/late_arrival_report.json  — lag stats + reprocessing manifest
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")

# Threshold: events arriving more than N hours after they occurred
# are considered "late" and need reprocessing
LATE_THRESHOLD_HOURS = {
    "events_news":     2,   # Web events are near-real-time
    "events_games":    4,   # Mobile-heavy, higher lag expected
    "events_cooking":  4,   # Mobile-heavy
    "events_athletic": 3,   # Mix of web and mobile
}

# If > this % of a day's events are late, flag that day for full reprocessing
REPROCESS_DAY_THRESHOLD_PCT = 5.0


def simulate_arrival_lag(df: pd.DataFrame, domain: str) -> pd.DataFrame:
    """
    Simulate realistic file arrival lag based on platform.
    In production, this is replaced by GCS object timeCreated metadata.

    Lag distribution (realistic mobile SDK behavior):
      - web:     near-zero (seconds)
      - ios:     0-2 hours (background sync)
      - android: 0-6 hours (Doze mode, battery optimization)
      - tablet:  0-1 hour
    """
    np.random.seed(42)
    df = df.copy()
    df["event_timestamp_parsed"] = pd.to_datetime(df["event_timestamp"], errors="coerce")

    # Assign lag based on platform
    lag_hours = np.where(
        df["platform"] == "web",
        np.random.exponential(0.05, len(df)),        # web: ~3 min avg
        np.where(
            df["platform"] == "ios",
            np.random.exponential(1.5, len(df)),     # iOS: ~1.5h avg
            np.random.exponential(3.5, len(df))      # android: ~3.5h avg
        )
    )
    df["simulated_arrival_lag_hours"] = lag_hours
    df["simulated_file_arrival_ts"] = (
        df["event_timestamp_parsed"] + pd.to_timedelta(lag_hours, unit="h")
    )
    df["event_date"] = df["event_timestamp_parsed"].dt.date

    threshold = LATE_THRESHOLD_HOURS.get(domain, 2)
    df["is_late_arrival"] = lag_hours > threshold
    df["lag_bucket"] = pd.cut(
        lag_hours,
        bins=[0, 0.5, 2, 6, 12, float("inf")],
        labels=["<30min", "30min-2h", "2-6h", "6-12h", ">12h"],
        right=True
    )
    return df


def analyze_domain(domain: str, df: pd.DataFrame) -> Dict:
    """Compute late arrival statistics for a single domain."""
    threshold = LATE_THRESHOLD_HOURS.get(domain, 2)
    total = len(df)
    late  = df["is_late_arrival"].sum()
    late_pct = round(late / total * 100, 2) if total > 0 else 0

    # Per-platform breakdown
    platform_stats = (
        df.groupby("platform")
          .agg(
              total_events=("event_id", "count"),
              late_events=("is_late_arrival", "sum"),
              avg_lag_hours=("simulated_arrival_lag_hours", "mean"),
              p95_lag_hours=("simulated_arrival_lag_hours", lambda x: x.quantile(0.95)),
          )
          .round(2)
          .reset_index()
          .to_dict("records")
    )

    # Per-day breakdown — find days needing reprocessing
    daily = (
        df.groupby("event_date")
          .agg(
              total=("event_id", "count"),
              late=("is_late_arrival", "sum"),
          )
          .reset_index()
    )
    daily["late_pct"] = (daily["late"] / daily["total"] * 100).round(2)
    daily["needs_reprocessing"] = daily["late_pct"] > REPROCESS_DAY_THRESHOLD_PCT
    reprocess_dates = daily[daily["needs_reprocessing"]]["event_date"].astype(str).tolist()

    # Lag bucket distribution
    bucket_dist = df["lag_bucket"].value_counts().sort_index().to_dict()
    bucket_dist = {str(k): int(v) for k, v in bucket_dist.items()}

    return {
        "domain":              domain,
        "threshold_hours":     threshold,
        "total_events":        int(total),
        "late_events":         int(late),
        "late_pct":            late_pct,
        "avg_lag_hours":       round(float(df["simulated_arrival_lag_hours"].mean()), 3),
        "p50_lag_hours":       round(float(df["simulated_arrival_lag_hours"].median()), 3),
        "p95_lag_hours":       round(float(df["simulated_arrival_lag_hours"].quantile(0.95)), 3),
        "max_lag_hours":       round(float(df["simulated_arrival_lag_hours"].max()), 3),
        "lag_bucket_dist":     bucket_dist,
        "platform_breakdown":  platform_stats,
        "reprocess_dates":     reprocess_dates,
        "reprocess_day_count": len(reprocess_dates),
    }


def generate_reprocessing_manifest(domain_results: List[Dict]) -> Dict:
    """
    Produce the manifest consumed by Airflow's reprocess_late_arrivals task group.
    For each domain+date pair needing reprocessing, Airflow will re-run
    the dbt incremental model with --full-refresh for that partition.
    """
    manifest_entries = []
    for result in domain_results:
        for date_str in result["reprocess_dates"]:
            manifest_entries.append({
                "domain":       result["domain"],
                "date":         date_str,
                "late_pct":     None,   # populated in production from actual query
                "dbt_command":  f"dbt run --select staging.{result['domain']} "
                                f"--vars '{{run_date: {date_str}, full_refresh: true}}'",
                "priority":     "high" if result["late_pct"] > 10 else "normal",
            })

    return {
        "generated_at":    datetime.now().isoformat(),
        "total_reprocess_jobs": len(manifest_entries),
        "entries":         manifest_entries,
    }


def main():
    print("=" * 65)
    print("NYT Analytics Engineering — Late Arrival Detector")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    datasets = {
        "events_games":   pd.read_parquet(os.path.join(DATA_DIR, "games",    "events.parquet")),
        "events_news":    pd.read_parquet(os.path.join(DATA_DIR, "news",     "events.parquet")),
        "events_cooking": pd.read_parquet(os.path.join(DATA_DIR, "cooking",  "events.parquet")),
        "events_athletic":pd.read_parquet(os.path.join(DATA_DIR, "athletic", "events.parquet")),
    }

    domain_results = []
    for domain, df in datasets.items():
        df_with_lag = simulate_arrival_lag(df, domain)
        result = analyze_domain(domain, df_with_lag)
        domain_results.append(result)

        status = "⚠️ " if result["late_pct"] > 2 else "✅"
        print(f"\n{status} {domain}")
        print(f"   Total events:  {result['total_events']:,}")
        print(f"   Late arrivals: {result['late_events']:,}  ({result['late_pct']}%)")
        print(f"   Avg lag:       {result['avg_lag_hours']}h  |  p95: {result['p95_lag_hours']}h  |  max: {result['max_lag_hours']:.1f}h")
        print(f"   Lag buckets:   {result['lag_bucket_dist']}")
        print(f"   Platform breakdown:")
        for p in result["platform_breakdown"]:
            print(f"     {p['platform']:8s}: avg_lag={p['avg_lag_hours']}h  p95={p['p95_lag_hours']}h  late={int(p['late_events'])} events")
        if result["reprocess_dates"]:
            print(f"   📅 Dates needing reprocessing ({result['reprocess_day_count']}): {result['reprocess_dates'][:5]}...")

    reprocess_manifest = generate_reprocessing_manifest(domain_results)

    report = {
        "generated_at":    datetime.now().isoformat(),
        "late_threshold":  LATE_THRESHOLD_HOURS,
        "reprocess_threshold_pct": REPROCESS_DAY_THRESHOLD_PCT,
        "summary": {
            "total_events":       sum(r["total_events"] for r in domain_results),
            "total_late":         sum(r["late_events"] for r in domain_results),
            "overall_late_pct":   round(sum(r["late_events"] for r in domain_results) /
                                        sum(r["total_events"] for r in domain_results) * 100, 2),
            "domains_with_reprocessing": sum(1 for r in domain_results if r["reprocess_dates"]),
            "total_reprocess_jobs": reprocess_manifest["total_reprocess_jobs"],
        },
        "domains":             domain_results,
        "reprocessing_manifest": reprocess_manifest,
    }

    out = os.path.join(BASE_DIR, "..", "data", "late_arrival_report.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n\n📄 Late arrival report → {out}")
    print(f"   Overall late rate: {report['summary']['overall_late_pct']}%")
    print(f"   Reprocessing jobs queued: {report['summary']['total_reprocess_jobs']}")
    print("\n✅ Reprocessing manifest written — Airflow will pick this up for t-1 and t-2 reruns.")


if __name__ == "__main__":
    main()
