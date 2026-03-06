import os
import sys
from google.cloud import bigquery

PROJECT_ID = "nyt-analytics-engineering"
DATASET_ID = "dbt_dev"

# The raw SQL translated from the dbt models
SQL = f"""
-- 1. Create the dataset
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}`;

-- 2. Create stg_subscribers
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.stg_subscribers` AS
SELECT
    CAST(subscriber_id AS STRING) AS subscriber_id,
    CAST(plan_type AS STRING) AS plan_type,
    CAST(country AS STRING) AS country,
    CAST(acquisition_channel AS STRING) AS acquisition_channel,
    CASE 
        WHEN CAST(plan_type AS STRING) = 'All Access' THEN 'full_bundle'
        WHEN CAST(plan_type AS STRING) IN ('Games + Cooking', 'News + Games') THEN 'partial_bundle'
        ELSE 'single_product'
    END AS bundle_tier
FROM `{PROJECT_ID}.raw_data.raw_subscribers`;

-- 3. Create mtr_daily_active_subscribers
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.mtr_daily_active_subscribers` AS
WITH
news_active AS (
    SELECT
        DATE(CAST(event_timestamp AS TIMESTAMP)) AS event_date,
        CAST(subscriber_id AS STRING) AS subscriber_id,
        'news' AS product,
        CAST(platform AS STRING) AS platform,
        'article_complete' AS qualifying_event
    FROM `{PROJECT_ID}.raw_data.raw_events_news`
    WHERE event_type = 'article_complete'
),
games_active AS (
    SELECT
        DATE(CAST(event_timestamp AS TIMESTAMP)) AS event_date,
        CAST(subscriber_id AS STRING) AS subscriber_id,
        'games' AS product,
        CAST(platform AS STRING) AS platform,
        'game_complete' AS qualifying_event
    FROM `{PROJECT_ID}.raw_data.raw_events_games`
    WHERE event_type = 'game_complete'
),
cooking_active AS (
    SELECT
        DATE(CAST(event_timestamp AS TIMESTAMP)) AS event_date,
        CAST(subscriber_id AS STRING) AS subscriber_id,
        'cooking' AS product,
        CAST(platform AS STRING) AS platform,
        'cook_mode_start' AS qualifying_event
    FROM `{PROJECT_ID}.raw_data.raw_events_cooking`
    WHERE event_type = 'cook_mode_start'
),
athletic_active AS (
    SELECT
        DATE(CAST(event_timestamp AS TIMESTAMP)) AS event_date,
        CAST(subscriber_id AS STRING) AS subscriber_id,
        'athletic' AS product,
        CAST(platform AS STRING) AS platform,
        'article_view_60s' AS qualifying_event
    FROM `{PROJECT_ID}.raw_data.raw_events_athletic`
    WHERE event_type = 'article_view'
      AND CAST(time_on_page_sec AS FLOAT64) > 60
),
all_active AS (
    SELECT * FROM news_active UNION ALL
    SELECT * FROM games_active UNION ALL
    SELECT * FROM cooking_active UNION ALL
    SELECT * FROM athletic_active
),
enriched AS (
    SELECT
        a.event_date,
        a.product,
        a.platform,
        a.qualifying_event,
        a.subscriber_id,
        s.plan_type,
        s.bundle_tier,
        CAST(s.country AS STRING) AS country,
        CAST(s.acquisition_channel AS STRING) AS acquisition_channel
    FROM all_active a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.stg_subscribers` s
        USING (subscriber_id)
),
aggregated AS (
    SELECT
        event_date,
        product,
        platform,
        bundle_tier,
        country,
        COUNT(DISTINCT subscriber_id) AS daily_active_subscribers,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'full_bundle' THEN subscriber_id END) AS daily_active_full_bundle,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'partial_bundle' THEN subscriber_id END) AS daily_active_partial_bundle,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'single_product' THEN subscriber_id END) AS daily_active_single_product
    FROM enriched
    GROUP BY 1,2,3,4,5
),
cross_product_total AS (
    SELECT
        event_date,
        'ALL_PRODUCTS' AS product,
        CAST(NULL AS STRING) AS platform,
        CAST(NULL AS STRING) AS bundle_tier,
        CAST(NULL AS STRING) AS country,
        COUNT(DISTINCT a.subscriber_id) AS daily_active_subscribers,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'full_bundle' THEN a.subscriber_id END) AS daily_active_full_bundle,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'partial_bundle' THEN a.subscriber_id END) AS daily_active_partial_bundle,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'single_product' THEN a.subscriber_id END) AS daily_active_single_product
    FROM all_active a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.stg_subscribers` s USING (subscriber_id)
    GROUP BY 1
)
SELECT
    *
FROM aggregated
UNION ALL
SELECT
    *
FROM cross_product_total
ORDER BY event_date DESC, product;
"""

def main():
    print("Building BigQuery models (bypassing local dbt memory limits)...")
    client = bigquery.Client(project=PROJECT_ID)
    
    # Split queries and run them
    queries = [q.strip() for q in SQL.split(';') if q.strip()]
    
    for query in queries:
        print(f"Executing: {query.splitlines()[0][:50]}...")
        job = client.query(query)
        job.result()
        
    print(f"\n✅ All models built successfully in {PROJECT_ID}.{DATASET_ID}")

if __name__ == "__main__":
    main()
