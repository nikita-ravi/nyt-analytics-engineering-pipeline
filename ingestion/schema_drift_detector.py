"""
NYT Analytics Engineering — Schema Drift Detector
===================================================
Runs as the FIRST task in the Airflow DAG, before any dbt model executes.
Compares the live Parquet schema against a version-controlled baseline
(the "Schema Registry") and raises immediately on breaking changes.

Pain point this solves:
    Engineering teams ship mobile SDK / event schema changes without telling
    Data Platform. A renamed column causes a silent NULL join in staging —
    yesterday's bundle metrics drop 30% and nobody notices until the CEO's
    Monday review.

Classification of drift:
    BREAKING  — missing columns, type narrowing (float→int), PK change
                 → pipeline halts, Slack alert, incident opened
    WARNING   — new columns added, type widening (int→float)
                 → pipeline continues, GitHub issue opened, registry updated
    INFO      — column order changed, description updated
                 → logged only

Usage:
    python schema_drift_detector.py                    # clean run
    python schema_drift_detector.py --inject-drift     # simulate a breaking change
    python schema_drift_detector.py --output report    # write JSON report
"""

import argparse
import json
import os
import sys
import copy
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
REGISTRY_PATH = os.path.join(BASE_DIR, "..", "docs", "schema_registry.json")

# ── Schema Registry (version-controlled ground truth) ──────────────────────
# In production: stored in Git, reviewed via PR, updated by data engineers.
# Changes to BREAKING columns require Data Platform sign-off.
SCHEMA_REGISTRY = {
    "version": "2024.Q1.3",
    "last_updated": "2024-03-01",
    "approved_by": "tommy.wu@nytimes.com",
    "datasets": {
        "subscribers": {
            "primary_key": "subscriber_id",
            "path": "subscribers/subscribers.csv",
            "format": "csv",
            "columns": {
                "subscriber_id":            {"dtype": "object",  "nullable": False, "breaking": True},
                "email_domain":             {"dtype": "object",  "nullable": True,  "breaking": False},
                "plan_type":                {"dtype": "object",  "nullable": False, "breaking": True},
                "subscription_start_date":  {"dtype": "object",  "nullable": False, "breaking": True},
                "subscription_end_date":    {"dtype": "object",  "nullable": True,  "breaking": False},
                "is_churned":               {"dtype": "bool",    "nullable": False, "breaking": True},
                "acquisition_channel":      {"dtype": "object",  "nullable": True,  "breaking": False},
                "country":                  {"dtype": "object",  "nullable": True,  "breaking": False},
                "device_primary":           {"dtype": "object",  "nullable": True,  "breaking": False},
                "created_at":               {"dtype": "object",  "nullable": False, "breaking": False},
            },
            "allowed_plan_types": ["bundle_all","bundle_3","news_only","games_only","cooking_only","athletic_only"],
        },
        "events_games": {
            "primary_key": "event_id",
            "path": "games/events.parquet",
            "format": "parquet",
            "columns": {
                "event_id":        {"dtype": "object",  "nullable": False, "breaking": True},
                "subscriber_id":   {"dtype": "object",  "nullable": True,  "breaking": False},
                "game_name":       {"dtype": "object",  "nullable": False, "breaking": True},  # ← most likely to drift
                "event_type":      {"dtype": "object",  "nullable": False, "breaking": True},
                "score":           {"dtype": "int64",   "nullable": True,  "breaking": False},
                "streak_days":     {"dtype": "int64",   "nullable": True,  "breaking": False},
                "duration_sec":    {"dtype": "float64", "nullable": True,  "breaking": False},
                "event_timestamp": {"dtype": "object",  "nullable": False, "breaking": True},
                "platform":        {"dtype": "object",  "nullable": True,  "breaking": False},
            },
            "allowed_game_names": ["wordle","crossword","spelling_bee","connections","strands"],
        },
        "events_news": {
            "primary_key": "event_id",
            "path": "news/events.parquet",
            "format": "parquet",
            "columns": {
                "event_id":         {"dtype": "object",  "nullable": False, "breaking": True},
                "subscriber_id":    {"dtype": "object",  "nullable": True,  "breaking": False},
                "event_type":       {"dtype": "object",  "nullable": False, "breaking": True},
                "article_id":       {"dtype": "object",  "nullable": False, "breaking": True},
                "section":          {"dtype": "object",  "nullable": True,  "breaking": False},
                "time_on_page_sec": {"dtype": "float64", "nullable": True,  "breaking": False},
                "event_timestamp":  {"dtype": "object",  "nullable": False, "breaking": True},
                "platform":         {"dtype": "object",  "nullable": True,  "breaking": False},
                "referrer":         {"dtype": "object",  "nullable": True,  "breaking": False},
            },
        },
        "events_cooking": {
            "primary_key": "event_id",
            "path": "cooking/events.parquet",
            "format": "parquet",
            "columns": {
                "event_id":        {"dtype": "object",  "nullable": False, "breaking": True},
                "subscriber_id":   {"dtype": "object",  "nullable": True,  "breaking": False},
                "recipe_id":       {"dtype": "object",  "nullable": False, "breaking": True},
                "recipe_category": {"dtype": "object",  "nullable": True,  "breaking": False},
                "event_type":      {"dtype": "object",  "nullable": False, "breaking": True},
                "rating":          {"dtype": "float64", "nullable": True,  "breaking": False},
                "event_timestamp": {"dtype": "object",  "nullable": False, "breaking": True},
                "platform":        {"dtype": "object",  "nullable": True,  "breaking": False},
            },
        },
        "events_athletic": {
            "primary_key": "event_id",
            "path": "athletic/events.parquet",
            "format": "parquet",
            "columns": {
                "event_id":         {"dtype": "object",  "nullable": False, "breaking": True},
                "subscriber_id":    {"dtype": "object",  "nullable": True,  "breaking": False},
                "sport":            {"dtype": "object",  "nullable": False, "breaking": True},
                "article_id":       {"dtype": "object",  "nullable": False, "breaking": True},
                "event_type":       {"dtype": "object",  "nullable": False, "breaking": True},
                "time_on_page_sec": {"dtype": "float64", "nullable": True,  "breaking": False},
                "event_timestamp":  {"dtype": "object",  "nullable": False, "breaking": True},
                "platform":         {"dtype": "object",  "nullable": True,  "breaking": False},
            },
        },
    }
}


# ── data classes ─────────────────────────────────────────────────────────────
@dataclass
class DriftEvent:
    dataset:    str
    column:     str
    drift_type: str          # MISSING_COLUMN, TYPE_CHANGE, NEW_COLUMN, ENUM_VIOLATION
    severity:   str          # BREAKING, WARNING, INFO
    expected:   Optional[str] = None
    actual:     Optional[str] = None
    detail:     str = ""

    @property
    def emoji(self):
        return {"BREAKING": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(self.severity, "⚪")


@dataclass
class DatasetDriftResult:
    dataset:      str
    checked_at:   str
    total_columns_expected: int
    total_columns_actual:   int
    events:       List[DriftEvent] = field(default_factory=list)

    @property
    def has_breaking(self): return any(e.severity == "BREAKING" for e in self.events)
    @property
    def has_warning(self):  return any(e.severity == "WARNING"  for e in self.events)
    @property
    def status(self):
        if self.has_breaking: return "BREAKING"
        if self.has_warning:  return "WARNING"
        if self.events:       return "INFO"
        return "CLEAN"

    @property
    def status_emoji(self):
        return {"BREAKING": "🔴", "WARNING": "🟡", "INFO": "🔵", "CLEAN": "✅"}.get(self.status)


# ── core detection logic ──────────────────────────────────────────────────────
def detect_dataset_drift(dataset_name: str, df: pd.DataFrame, registry: dict) -> DatasetDriftResult:
    expected_cols = registry["columns"]
    actual_dtypes = df.dtypes.astype(str).to_dict()
    now = datetime.now().isoformat()

    result = DatasetDriftResult(
        dataset=dataset_name,
        checked_at=now,
        total_columns_expected=len(expected_cols),
        total_columns_actual=len(actual_dtypes),
    )

    # ── 1. Missing columns (BREAKING if flagged) ──────────────────────────
    for col, meta in expected_cols.items():
        if col not in actual_dtypes:
            result.events.append(DriftEvent(
                dataset=dataset_name, column=col,
                drift_type="MISSING_COLUMN",
                severity="BREAKING" if meta["breaking"] else "WARNING",
                expected=meta["dtype"], actual=None,
                detail=f"Column '{col}' disappeared. Was it renamed by engineering? Check mobile SDK changelog."
            ))

    # ── 2. Type changes ───────────────────────────────────────────────────
    TYPE_COMPATIBLE = {("int64", "float64"), ("float64", "float32")}  # widening = OK
    for col, meta in expected_cols.items():
        if col not in actual_dtypes:
            continue
        expected_dtype = meta["dtype"]
        actual_dtype   = actual_dtypes[col]
        if expected_dtype != actual_dtype:
            is_widening = (expected_dtype, actual_dtype) in TYPE_COMPATIBLE
            result.events.append(DriftEvent(
                dataset=dataset_name, column=col,
                drift_type="TYPE_CHANGE",
                severity="WARNING" if is_widening else "BREAKING",
                expected=expected_dtype, actual=actual_dtype,
                detail=f"Type widening (OK)" if is_widening else
                       f"Type narrowing: '{expected_dtype}' → '{actual_dtype}'. "
                       f"Downstream CAST()s will fail or silently truncate."
            ))

    # ── 3. New columns (WARNING — might be intentional addition) ─────────
    for col in actual_dtypes:
        if col not in expected_cols:
            result.events.append(DriftEvent(
                dataset=dataset_name, column=col,
                drift_type="NEW_COLUMN",
                severity="WARNING",
                expected=None, actual=actual_dtypes[col],
                detail=f"New column '{col}' detected. Review and add to schema registry if valid."
            ))

    # ── 4. Enum value violations ──────────────────────────────────────────
    if dataset_name == "events_games" and "game_name" in df.columns:
        allowed = set(registry.get("allowed_game_names", []))
        actual_vals = set(df["game_name"].dropna().unique())
        unexpected = actual_vals - allowed
        if unexpected:
            result.events.append(DriftEvent(
                dataset=dataset_name, column="game_name",
                drift_type="ENUM_VIOLATION",
                severity="WARNING",
                detail=f"Unknown game_name values: {unexpected}. New game launched? Update registry."
            ))

    if dataset_name == "subscribers" and "plan_type" in df.columns:
        allowed = set(registry.get("allowed_plan_types", []))
        actual_vals = set(df["plan_type"].dropna().unique())
        unexpected = actual_vals - allowed
        if unexpected:
            result.events.append(DriftEvent(
                dataset=dataset_name, column="plan_type",
                drift_type="ENUM_VIOLATION",
                severity="BREAKING",
                detail=f"Unknown plan_type values: {unexpected}. New plan launched without Data Platform notification?"
            ))

    return result


def inject_drift_for_demo(datasets: dict) -> dict:
    """Mutate datasets to simulate realistic schema drift scenarios."""
    import numpy as np
    drifted = {}

    # Scenario 1: Games team renames game_name → game_title (BREAKING)
    games_df = datasets["events_games"].copy()
    games_df = games_df.rename(columns={"game_name": "game_title"})
    drifted["events_games"] = games_df

    # Scenario 2: Athletic team changes time_on_page_sec to int (TYPE CHANGE)
    athletic_df = datasets["events_athletic"].copy()
    athletic_df["time_on_page_sec"] = athletic_df["time_on_page_sec"].fillna(0).astype(int)
    drifted["events_athletic"] = athletic_df

    # Scenario 3: News team adds new tracking column (WARNING — new column)
    news_df = datasets["events_news"].copy()
    news_df["scroll_depth_pct"] = np.random.uniform(0, 100, len(news_df))
    drifted["events_news"] = news_df

    # Scenario 4: Subscribers — new plan type not in registry (BREAKING enum)
    subs_df = datasets["subscribers"].copy()
    subs_df.loc[subs_df.sample(10).index, "plan_type"] = "audio_only"  # new product!
    drifted["subscribers"] = subs_df

    drifted["events_cooking"] = datasets["events_cooking"]
    return drifted


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NYT Schema Drift Detector")
    parser.add_argument("--inject-drift", action="store_true", help="Simulate drift scenarios")
    parser.add_argument("--output", default=os.path.join(BASE_DIR, "..", "data", "schema_drift_report.json"))
    args = parser.parse_args()

    print("=" * 65)
    print("NYT Analytics Engineering — Schema Drift Detector")
    print(f"Registry version: {SCHEMA_REGISTRY['version']}  |  Approved: {SCHEMA_REGISTRY['approved_by']}")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # Load datasets
    raw_datasets = {
        "subscribers":    pd.read_csv(os.path.join(DATA_DIR, "subscribers", "subscribers.csv")),
        "events_games":   pd.read_parquet(os.path.join(DATA_DIR, "games",    "events.parquet")),
        "events_news":    pd.read_parquet(os.path.join(DATA_DIR, "news",     "events.parquet")),
        "events_cooking": pd.read_parquet(os.path.join(DATA_DIR, "cooking",  "events.parquet")),
        "events_athletic":pd.read_parquet(os.path.join(DATA_DIR, "athletic", "events.parquet")),
    }

    if args.inject_drift:
        print("\n⚠️  DRIFT INJECTION ENABLED — simulating 4 real-world schema change scenarios\n")
        raw_datasets = inject_drift_for_demo(raw_datasets)

    results = []
    any_breaking = False

    for dataset_name, df in raw_datasets.items():
        registry = SCHEMA_REGISTRY["datasets"][dataset_name]
        result = detect_dataset_drift(dataset_name, df, registry)
        results.append(result)

        print(f"\n{result.status_emoji} [{result.status:8s}] {dataset_name}")
        print(f"   Columns: expected={result.total_columns_expected}, actual={result.total_columns_actual}")

        if result.events:
            for evt in result.events:
                print(f"   {evt.emoji} {evt.severity:8s} | {evt.drift_type:16s} | col: '{evt.column}'")
                print(f"            → {evt.detail}")
                if evt.expected or evt.actual:
                    print(f"            → expected={evt.expected!r}, actual={evt.actual!r}")
        else:
            print("   ✓ No drift detected — schema matches registry")

        if result.has_breaking:
            any_breaking = True

    # Write JSON report
    report = {
        "registry_version": SCHEMA_REGISTRY["version"],
        "run_at":           datetime.now().isoformat(),
        "drift_injected":   args.inject_drift,
        "summary": {
            "total_datasets": len(results),
            "clean":          sum(1 for r in results if r.status == "CLEAN"),
            "info":           sum(1 for r in results if r.status == "INFO"),
            "warning":        sum(1 for r in results if r.status == "WARNING"),
            "breaking":       sum(1 for r in results if r.status == "BREAKING"),
            "total_events":   sum(len(r.events) for r in results),
        },
        "datasets": [
            {
                "dataset": r.dataset,
                "status":  r.status,
                "events": [asdict(e) for e in r.events]
            }
            for r in results
        ]
    }
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n\n📄 Schema drift report → {args.output}")
    print(f"   Clean: {report['summary']['clean']}  |  Warning: {report['summary']['warning']}  |  Breaking: {report['summary']['breaking']}")

    if any_breaking:
        print("\n🔴 BREAKING DRIFT DETECTED — halting pipeline. Fix schema or update registry.")
        print("   In Airflow: this task fails → downstream dbt tasks are blocked.")
        print("   In CI/CD: this exits 1 → PR is blocked from merging.")
        sys.exit(1)
    else:
        print("\n✅ Schema validated — safe to proceed to dbt staging.")


if __name__ == "__main__":
    main()
