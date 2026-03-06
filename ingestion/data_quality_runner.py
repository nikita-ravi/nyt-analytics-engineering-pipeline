"""
NYT Analytics Engineering — Data Quality Framework
====================================================
Standalone Python DQ runner that mirrors what the dbt mart computes,
but executes directly against Parquet files (for local dev / CI validation
before pushing to BigQuery).

Features:
  • 5-dimension DQ scoring (completeness, uniqueness, timeliness, referential, validity)
  • Weighted composite DQ score per dataset
  • PASS / WARN / FAIL thresholds
  • JSON report output (consumed by dashboard)
  • Exit code 1 on any FAIL (for CI/CD gate integration)

Usage:
    python data_quality_runner.py
    python data_quality_runner.py --output dq_report.json
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pandas as pd

# ── config ─────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "..", "data", "raw")

DQ_WEIGHTS = {
    "completeness":   0.30,
    "uniqueness":     0.20,
    "timeliness":     0.15,
    "referential":    0.20,
    "validity":       0.15,
}
PASS_THRESHOLD = 0.95
WARN_THRESHOLD = 0.85


# ── data classes ────────────────────────────────────────────────────────────
@dataclass
class DQDimension:
    name: str
    score: float          # 0.0 – 1.0
    issues: List[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        if self.score >= PASS_THRESHOLD:
            return "PASS"
        elif self.score >= WARN_THRESHOLD:
            return "WARN"
        return "FAIL"


@dataclass
class DatasetDQResult:
    dataset_name: str
    domain: str
    run_at: str
    total_records: int
    dimensions: List[DQDimension]

    @property
    def composite_score(self) -> float:
        dim_map = {d.name: d.score for d in self.dimensions}
        return round(sum(
            dim_map.get(name, 0.0) * weight
            for name, weight in DQ_WEIGHTS.items()
        ), 4)

    @property
    def status(self) -> str:
        s = self.composite_score
        if s >= PASS_THRESHOLD: return "PASS"
        if s >= WARN_THRESHOLD: return "WARN"
        return "FAIL"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name":    self.dataset_name,
            "domain":          self.domain,
            "run_at":          self.run_at,
            "total_records":   self.total_records,
            "composite_score": round(self.composite_score * 100, 2),
            "status":          self.status,
            "dimensions": {
                d.name: {
                    "score":  round(d.score * 100, 2),
                    "label":  d.label,
                    "issues": d.issues,
                }
                for d in self.dimensions
            }
        }


# ── core DQ checks ──────────────────────────────────────────────────────────
def check_completeness(df: pd.DataFrame, key_cols: List[str]) -> DQDimension:
    """Fraction of non-null values across key columns."""
    issues = []
    scores = []
    for col in key_cols:
        if col not in df.columns:
            issues.append(f"Column '{col}' missing entirely from dataset")
            scores.append(0.0)
        else:
            null_pct = df[col].isnull().mean()
            scores.append(1.0 - null_pct)
            if null_pct > 0.01:
                issues.append(f"'{col}': {null_pct:.1%} null values ({int(null_pct * len(df))} records)")
    return DQDimension("completeness", sum(scores) / len(scores) if scores else 0.0, issues)


def check_uniqueness(df: pd.DataFrame, pk_col: str) -> DQDimension:
    """Ratio of distinct primary key values to total rows."""
    if pk_col not in df.columns:
        return DQDimension("uniqueness", 0.0, [f"PK column '{pk_col}' not found"])
    total    = len(df)
    distinct = df[pk_col].nunique()
    dupes    = total - distinct
    score    = distinct / total if total > 0 else 1.0
    issues   = [f"{dupes:,} duplicate '{pk_col}' values detected"] if dupes > 0 else []
    return DQDimension("uniqueness", score, issues)


def check_timeliness(df: pd.DataFrame, ts_col: str) -> DQDimension:
    """Fraction of records with timestamps ≤ now (no future timestamps)."""
    if ts_col not in df.columns:
        return DQDimension("timeliness", 1.0, [])  # N/A
    now = pd.Timestamp.now()
    ts  = pd.to_datetime(df[ts_col], errors="coerce")
    future_count = (ts > now).sum()
    null_count   = ts.isnull().sum()
    total        = len(df)
    issues       = []
    if future_count > 0:
        issues.append(f"{future_count:,} records have future timestamps (clock skew?)")
    if null_count > 0:
        issues.append(f"{null_count:,} records have unparseable timestamps")
    score = 1.0 - (future_count + null_count) / total if total > 0 else 1.0
    return DQDimension("timeliness", score, issues)


def check_referential_integrity(
    df: pd.DataFrame, fk_col: str, ref_df: pd.DataFrame, ref_col: str
) -> DQDimension:
    """Fraction of FK values that exist in the reference dataset."""
    if fk_col not in df.columns:
        return DQDimension("referential", 1.0, [])
    valid   = df[fk_col].isin(ref_df[ref_col])
    orphans = (~valid & df[fk_col].notna()).sum()
    nulls   = df[fk_col].isnull().sum()
    total   = len(df)
    issues  = []
    if orphans > 0:
        issues.append(f"{orphans:,} orphaned records (FK not in subscriber master)")
    if nulls > 0:
        issues.append(f"{nulls:,} records with null '{fk_col}'")
    score = 1.0 - (orphans + nulls) / total if total > 0 else 1.0
    return DQDimension("referential", score, issues)


def check_validity(
    df: pd.DataFrame, col: str, allowed_values: List[str]
) -> DQDimension:
    """Fraction of a column's values that match the allowed set."""
    if col not in df.columns:
        return DQDimension("validity", 0.0, [f"Column '{col}' missing"])
    invalid = ~df[col].isin(allowed_values) & df[col].notna()
    n_invalid = invalid.sum()
    total = len(df)
    issues = []
    if n_invalid > 0:
        bad_vals = df.loc[invalid, col].value_counts().head(5).to_dict()
        issues.append(f"{n_invalid:,} records with invalid '{col}' values: {bad_vals}")
    score = 1.0 - n_invalid / total if total > 0 else 1.0
    return DQDimension("validity", score, issues)


# ── dataset-specific runners ────────────────────────────────────────────────
def run_subscriber_dq(subs_df: pd.DataFrame) -> DatasetDQResult:
    return DatasetDQResult(
        dataset_name="subscribers",
        domain="subscriber_domain",
        run_at=datetime.now().isoformat(),
        total_records=len(subs_df),
        dimensions=[
            check_completeness(subs_df, ["subscriber_id","plan_type","subscription_start_date","is_churned"]),
            check_uniqueness(subs_df, "subscriber_id"),
            check_timeliness(subs_df, "created_at"),
            DQDimension("referential", 1.0),  # root entity — no FK
            check_validity(subs_df, "plan_type",
                           ["bundle_all","bundle_3","news_only","games_only",
                            "cooking_only","athletic_only","wirecutter_only"]),
        ]
    )


def run_news_dq(news_df: pd.DataFrame, subs_df: pd.DataFrame) -> DatasetDQResult:
    return DatasetDQResult(
        dataset_name="events_news",
        domain="news_domain",
        run_at=datetime.now().isoformat(),
        total_records=len(news_df),
        dimensions=[
            check_completeness(news_df, ["event_id","event_type","event_timestamp"]),
            check_uniqueness(news_df, "event_id"),
            check_timeliness(news_df, "event_timestamp"),
            check_referential_integrity(news_df, "subscriber_id", subs_df, "subscriber_id"),
            check_validity(news_df, "event_type",
                           ["article_view","article_complete","share","save","comment"]),
        ]
    )


def run_games_dq(games_df: pd.DataFrame, subs_df: pd.DataFrame) -> DatasetDQResult:
    return DatasetDQResult(
        dataset_name="events_games",
        domain="games_domain",
        run_at=datetime.now().isoformat(),
        total_records=len(games_df),
        dimensions=[
            check_completeness(games_df, ["event_id","game_name","event_timestamp"]),
            check_uniqueness(games_df, "event_id"),
            check_timeliness(games_df, "event_timestamp"),
            check_referential_integrity(games_df, "subscriber_id", subs_df, "subscriber_id"),
            check_validity(games_df, "game_name",
                           ["wordle","crossword","spelling_bee","connections","strands"]),
        ]
    )


def run_cooking_dq(cooking_df: pd.DataFrame, subs_df: pd.DataFrame) -> DatasetDQResult:
    return DatasetDQResult(
        dataset_name="events_cooking",
        domain="cooking_domain",
        run_at=datetime.now().isoformat(),
        total_records=len(cooking_df),
        dimensions=[
            check_completeness(cooking_df, ["event_id","recipe_id","event_timestamp"]),
            check_uniqueness(cooking_df, "event_id"),
            check_timeliness(cooking_df, "event_timestamp"),
            check_referential_integrity(cooking_df, "subscriber_id", subs_df, "subscriber_id"),
            check_validity(cooking_df, "event_type",
                           ["view","save","cook_mode_start","cook_mode_complete","rating_submit","share"]),
        ]
    )


def run_athletic_dq(athletic_df: pd.DataFrame, subs_df: pd.DataFrame) -> DatasetDQResult:
    return DatasetDQResult(
        dataset_name="events_athletic",
        domain="athletic_domain",
        run_at=datetime.now().isoformat(),
        total_records=len(athletic_df),
        dimensions=[
            check_completeness(athletic_df, ["event_id","sport","event_timestamp"]),
            check_uniqueness(athletic_df, "event_id"),
            check_timeliness(athletic_df, "event_timestamp"),
            check_referential_integrity(athletic_df, "subscriber_id", subs_df, "subscriber_id"),
            check_validity(athletic_df, "event_type",
                           ["article_view","liveblog_view","podcast_play","video_play","comment"]),
        ]
    )


# ── main ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NYT Data Quality Runner")
    parser.add_argument("--output", default=os.path.join(BASE_DIR, "..", "data", "dq_report.json"))
    parser.add_argument("--use-dirty", action="store_true",
                        help="Use dirty datasets (with injected DQ issues) for demo")
    args = parser.parse_args()

    print("=" * 60)
    print("NYT Analytics Engineering — Data Quality Framework")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load datasets
    subs_file = "subscribers_dirty.csv" if args.use_dirty else "subscribers.csv"
    subs_df   = pd.read_csv(os.path.join(DATA_DIR, "subscribers", subs_file))

    news_suffix = "_dirty" if args.use_dirty else ""
    news_df     = pd.read_parquet(os.path.join(DATA_DIR, "news",     f"events{news_suffix}.parquet"))

    games_suffix = "_dirty" if args.use_dirty else ""
    games_df    = pd.read_parquet(os.path.join(DATA_DIR, "games",    f"events{games_suffix}.parquet"))
    cooking_df  = pd.read_parquet(os.path.join(DATA_DIR, "cooking",  "events.parquet"))
    athletic_df = pd.read_parquet(os.path.join(DATA_DIR, "athletic", "events.parquet"))

    print(f"\nLoaded {len(subs_df):,} subscribers, "
          f"{len(news_df)+len(games_df)+len(cooking_df)+len(athletic_df):,} total events\n")

    # Run DQ
    results = [
        run_subscriber_dq(subs_df),
        run_news_dq(news_df, subs_df),
        run_games_dq(games_df, subs_df),
        run_cooking_dq(cooking_df, subs_df),
        run_athletic_dq(athletic_df, subs_df),
    ]

    # Print results
    any_fail = False
    for r in results:
        status_icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[r.status]
        print(f"{status_icon} [{r.status}] {r.dataset_name:25s}  DQ Score: {r.composite_score*100:.1f}%  ({r.total_records:,} records)")
        for d in r.dimensions:
            dim_icon = {"PASS": "  ✓", "WARN": "  ⚠", "FAIL": "  ✗"}[d.label]
            print(f"   {dim_icon} {d.name:15s}: {d.score*100:.1f}%")
            for issue in d.issues:
                print(f"        ↳ {issue}")
        if r.status == "FAIL":
            any_fail = True
        print()

    # Write JSON report
    report = {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "total_datasets": len(results),
            "pass":  sum(1 for r in results if r.status == "PASS"),
            "warn":  sum(1 for r in results if r.status == "WARN"),
            "fail":  sum(1 for r in results if r.status == "FAIL"),
            "min_score": round(min(r.composite_score for r in results) * 100, 2),
            "avg_score": round(sum(r.composite_score for r in results) / len(results) * 100, 2),
        },
        "datasets": [r.to_dict() for r in results],
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"📄 DQ report written to: {args.output}")

    if any_fail:
        print("\n❌ DQ FAIL detected — exiting with code 1 (CI gate would block deployment)")
        sys.exit(1)
    else:
        print("\n✅ All datasets meet quality thresholds")


if __name__ == "__main__":
    main()
