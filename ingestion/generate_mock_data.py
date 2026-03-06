"""
NYT Analytics Engineering Demo
================================
Mock data generator for NYT subscription & engagement domain.
Produces Parquet + CSV outputs mirroring what raw event streams
and CRM systems would deliver to a GCS landing zone.

Products modeled:
  - news       (core digital subscription)
  - games      (Wordle, Crossword, Spelling Bee, Connections)
  - cooking    (recipe engagement, saves, completions)
  - athletic   (sports articles, live scores pages)
  - wirecutter (product review clicks, affiliate links)

Output: data/raw/ directory (mirrors GCS bucket structure)
"""

import os, random, uuid, json
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

# ── configuration ──────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
N_USERS    = 5_000          # total subscriber universe
N_DAYS     = 90             # 90-day lookback window
START_DATE = date.today() - timedelta(days=N_DAYS)

PRODUCTS   = ["news", "games", "cooking", "athletic", "wirecutter"]
PLAN_TYPES = {
    "bundle_all":    0.30,
    "bundle_3":      0.20,
    "news_only":     0.25,
    "games_only":    0.12,
    "cooking_only":  0.08,
    "athletic_only": 0.05,
}
CHURN_RATES = {
    "bundle_all":    0.008,
    "bundle_3":      0.012,
    "news_only":     0.024,
    "games_only":    0.030,
    "cooking_only":  0.022,
    "athletic_only":0.040,   # historically high as noted in JD research
}
GAMES_TITLES = ["wordle", "crossword", "spelling_bee", "connections", "strands"]
CONTENT_SECTIONS = ["politics", "technology", "arts", "sports", "science", "opinion", "world"]

os.makedirs(OUTPUT_DIR, exist_ok=True)
for product in PRODUCTS:
    os.makedirs(os.path.join(OUTPUT_DIR, product), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "subscribers"), exist_ok=True)


# ── helpers ────────────────────────────────────────────────────────────────
def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_ts(d: date) -> datetime:
    return datetime(d.year, d.month, d.day,
                    random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))

def weighted_choice(d: dict):
    keys, weights = zip(*d.items())
    return random.choices(keys, weights=list(weights), k=1)[0]


# ── 1. subscribers (dim table) ─────────────────────────────────────────────
print("Generating subscribers...")
plans = list(PLAN_TYPES.keys())
plan_weights = list(PLAN_TYPES.values())

subscribers = []
for i in range(N_USERS):
    sub_id    = str(uuid.uuid4())
    plan      = random.choices(plans, weights=plan_weights, k=1)[0]
    start_dt  = rand_date(START_DATE - timedelta(days=365), START_DATE + timedelta(days=N_DAYS - 10))
    is_churned = random.random() < CHURN_RATES[plan] * N_DAYS
    churn_dt  = rand_date(start_dt + timedelta(days=7), date.today()) if is_churned else None
    subscribers.append({
        "subscriber_id":    sub_id,
        "email_domain":     random.choice(["gmail.com","yahoo.com","icloud.com","outlook.com","edu"]),
        "plan_type":        plan,
        "subscription_start_date": start_dt.isoformat(),
        "subscription_end_date":   churn_dt.isoformat() if churn_dt else None,
        "is_churned":       is_churned,
        "acquisition_channel": random.choice(["organic","email","social","paid_search","app_store","referral"]),
        "country":          random.choices(["US","GB","CA","AU","IN"], weights=[0.70,0.10,0.08,0.06,0.06])[0],
        "device_primary":   random.choice(["mobile","desktop","tablet"]),
        "created_at":       rand_ts(START_DATE).isoformat(),
    })

df_subs = pd.DataFrame(subscribers)
df_subs.to_csv(os.path.join(OUTPUT_DIR, "subscribers", "subscribers.csv"), index=False)
df_subs.to_parquet(os.path.join(OUTPUT_DIR, "subscribers", "subscribers.parquet"), index=False)
print(f"  → {len(df_subs):,} subscribers written")


# ── 2. news article events ─────────────────────────────────────────────────
print("Generating news events...")
news_events = []
for _, sub in df_subs.iterrows():
    if sub["plan_type"] not in ("bundle_all","bundle_3","news_only"):
        if random.random() > 0.15:   # occasional cross-plan browsing
            continue
    n_sessions = random.randint(0, 25) if not sub["is_churned"] else random.randint(0, 5)
    for _ in range(n_sessions):
        evt_date = rand_date(START_DATE, date.today())
        news_events.append({
            "event_id":         str(uuid.uuid4()),
            "subscriber_id":    sub["subscriber_id"],
            "event_type":       random.choices(["article_view","article_complete","share","save","comment"],
                                               weights=[0.50,0.25,0.08,0.12,0.05])[0],
            "article_id":       f"art_{random.randint(1000,9999)}",
            "section":          random.choice(CONTENT_SECTIONS),
            "time_on_page_sec": np.random.lognormal(4.5, 1.2) if random.random() > 0.1 else None,  # intentional NULLs
            "event_timestamp":  rand_ts(evt_date).isoformat(),
            "platform":         random.choice(["web","ios","android"]),
            "referrer":         random.choice(["direct","search","social","newsletter","push_notif"]),
        })

df_news = pd.DataFrame(news_events)
df_news.to_parquet(os.path.join(OUTPUT_DIR, "news", "events.parquet"), index=False)
print(f"  → {len(df_news):,} news events written")


# ── 3. games events ────────────────────────────────────────────────────────
print("Generating games events...")
games_events = []
for _, sub in df_subs.iterrows():
    if sub["plan_type"] not in ("bundle_all","bundle_3","games_only"):
        if random.random() > 0.25:
            continue
    n_plays = random.randint(0, 60)
    for _ in range(n_plays):
        evt_date = rand_date(START_DATE, date.today())
        game    = random.choice(GAMES_TITLES)
        streak  = random.randint(0, 120)
        games_events.append({
            "event_id":      str(uuid.uuid4()),
            "subscriber_id": sub["subscriber_id"],
            "game_name":     game,
            "event_type":    random.choices(["game_start","game_complete","game_share","streak_milestone"],
                                            weights=[0.30,0.50,0.10,0.10])[0],
            "score":         random.randint(1, 6) if game == "wordle" else random.randint(50, 100),
            "streak_days":   streak,
            "duration_sec":  abs(np.random.normal(180, 90)),
            "event_timestamp": rand_ts(evt_date).isoformat(),
            "platform":      random.choice(["web","ios","android"]),
        })

df_games = pd.DataFrame(games_events)
df_games.to_parquet(os.path.join(OUTPUT_DIR, "games", "events.parquet"), index=False)
print(f"  → {len(df_games):,} games events written")


# ── 4. cooking events ──────────────────────────────────────────────────────
print("Generating cooking events...")
cooking_events = []
RECIPE_CATS = ["quick_dinners","baking","vegetarian","global_cuisine","desserts","drinks"]
for _, sub in df_subs.iterrows():
    if sub["plan_type"] not in ("bundle_all","bundle_3","cooking_only"):
        if random.random() > 0.20:
            continue
    n_actions = random.randint(0, 30)
    for _ in range(n_actions):
        evt_date = rand_date(START_DATE, date.today())
        cooking_events.append({
            "event_id":      str(uuid.uuid4()),
            "subscriber_id": sub["subscriber_id"],
            "recipe_id":     f"rec_{random.randint(1, 3000)}",
            "recipe_category": random.choice(RECIPE_CATS),
            "event_type":    random.choices(["view","save","cook_mode_start","cook_mode_complete","rating_submit","share"],
                                            weights=[0.35,0.25,0.15,0.10,0.10,0.05])[0],
            "rating":        random.randint(1,5) if random.random() < 0.3 else None,
            "event_timestamp": rand_ts(evt_date).isoformat(),
            "platform":      random.choice(["web","ios","android"]),
        })

df_cooking = pd.DataFrame(cooking_events)
df_cooking.to_parquet(os.path.join(OUTPUT_DIR, "cooking", "events.parquet"), index=False)
print(f"  → {len(df_cooking):,} cooking events written")


# ── 5. athletic events ─────────────────────────────────────────────────────
print("Generating Athletic events...")
athletic_events = []
SPORTS = ["nfl","nba","mlb","soccer","nhl","college"]
for _, sub in df_subs.iterrows():
    if sub["plan_type"] not in ("bundle_all","bundle_3","athletic_only"):
        if random.random() > 0.18:
            continue
    n_actions = random.randint(0, 40)
    for _ in range(n_actions):
        evt_date = rand_date(START_DATE, date.today())
        athletic_events.append({
            "event_id":       str(uuid.uuid4()),
            "subscriber_id":  sub["subscriber_id"],
            "sport":          random.choice(SPORTS),
            "article_id":     f"ath_{random.randint(1000, 8000)}",
            "event_type":     random.choices(["article_view","liveblog_view","podcast_play","video_play","comment"],
                                             weights=[0.45,0.25,0.15,0.10,0.05])[0],
            "time_on_page_sec": abs(np.random.normal(240, 120)),
            "event_timestamp":  rand_ts(evt_date).isoformat(),
            "platform":         random.choice(["web","ios","android"]),
        })

df_athletic = pd.DataFrame(athletic_events)
df_athletic.to_parquet(os.path.join(OUTPUT_DIR, "athletic", "events.parquet"), index=False)
print(f"  → {len(df_athletic):,} athletic events written")


# ── 6. introduce intentional data quality issues ───────────────────────────
print("Injecting data quality anomalies for DQ framework demo...")

# Duplicate subscriber records (simulates upstream CRM bug)
dupes = df_subs.sample(50, random_state=1).copy()
dupes["created_at"] = pd.Timestamp.now().isoformat()
df_subs_dirty = pd.concat([df_subs, dupes], ignore_index=True)
df_subs_dirty.to_csv(os.path.join(OUTPUT_DIR, "subscribers", "subscribers_dirty.csv"), index=False)

# Some news events missing subscriber_id (orphaned events)
df_news_dirty = df_news.copy()
orphan_idx = df_news_dirty.sample(int(0.02 * len(df_news_dirty)), random_state=2).index
df_news_dirty.loc[orphan_idx, "subscriber_id"] = None
df_news_dirty.to_parquet(os.path.join(OUTPUT_DIR, "news", "events_dirty.parquet"), index=False)

# Games events with future timestamps (pipeline clock skew bug)
df_games_dirty = df_games.copy()
future_idx = df_games_dirty.sample(30, random_state=3).index
df_games_dirty.loc[future_idx, "event_timestamp"] = (
    datetime.now() + timedelta(days=random.randint(1, 30))
).isoformat()
df_games_dirty.to_parquet(os.path.join(OUTPUT_DIR, "games", "events_dirty.parquet"), index=False)

print("  → Dirty datasets written for DQ validation demo")


# ── 7. write manifest ──────────────────────────────────────────────────────
manifest = {
    "generated_at":   datetime.now().isoformat(),
    "n_users":        N_USERS,
    "n_days":         N_DAYS,
    "start_date":     START_DATE.isoformat(),
    "record_counts":  {
        "subscribers":     len(df_subs),
        "news_events":     len(df_news),
        "games_events":    len(df_games),
        "cooking_events":  len(df_cooking),
        "athletic_events": len(df_athletic),
    },
    "dq_anomalies": {
        "duplicate_subscribers":  50,
        "orphaned_news_events":   int(0.02 * len(df_news)),
        "future_timestamp_games": 30,
    }
}
with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2)

print("\n✅ Data generation complete.")
print(json.dumps(manifest["record_counts"], indent=2))
