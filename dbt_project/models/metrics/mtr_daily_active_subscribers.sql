-- =============================================================================
-- mtr_daily_active_subscribers.sql
-- =============================================================================
-- OFFICIAL NYT METRIC DEFINITION: Daily Active Subscribers (DAS)
-- =============================================================================
-- ⚠️  THIS IS THE SINGLE SOURCE OF TRUTH FOR DAU/DAS AT THE NEW YORK TIMES ⚠️
-- Approved by: Data Platform, Finance, Product Analytics, Editorial Analytics
-- Version: 2024.Q1
-- Owner: analytics-engineering@nytimes.com
-- PR: github.com/nytimes/data-platform/pull/1847
-- =============================================================================
--
-- PROBLEM THIS SOLVES:
-- Before this model, five teams had five definitions of "daily active":
--   • Games team:     COUNT(game_start events)          → inflated (counts retries)
--   • News team:      COUNT(article_view events)        → inflated (counts bots)
--   • Athletic team:  COUNT(liveblog_view events)       → arbitrary threshold
--   • Finance:        active subscription day regardless of any engagement
--   • Editorial:      "any pageview including homepage" → wildly inflated
--
-- Result: Monday exec review shows 4 different DAU numbers, nobody knows which
-- to trust, decisions are delayed, cross-team alignment breaks down.
--
-- OFFICIAL DEFINITION:
--   A subscriber is "Daily Active" on date D if they completed at least ONE
--   qualifying engagement event on date D, where qualifying is defined as:
--
--   Product    | Qualifying Event           | Rationale
--   -----------|---------------------------|----------------------------------
--   News       | article_complete           | Reading = completing, not just landing
--   Games      | game_complete              | Attempting = intentional, not rage-click
--   Cooking    | cook_mode_start            | Intent to cook = high-value engagement
--   Athletic   | article_view              | Adjusted: liveblogs are short-form
--              | + time_on_page > 60s       | Quality gate to exclude bots/bounces
--
-- GRAIN: one row per (event_date, product, plan_type, platform, country)
-- MATERIALIZATION: Table, partitioned by event_date
-- =============================================================================

WITH

-- ── Qualifying news events ──────────────────────────────────────────────────
news_active AS (
    SELECT
        event_date,
        subscriber_id,
        'news'         AS product,
        platform,
        'article_complete' AS qualifying_event
    FROM {{ ref('stg_events_news') }}
    WHERE event_type = 'article_complete'
      AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

-- ── Qualifying games events ─────────────────────────────────────────────────
games_active AS (
    SELECT
        DATE(event_timestamp)  AS event_date,
        subscriber_id,
        'games'        AS product,
        platform,
        'game_complete' AS qualifying_event
    FROM {{ source('raw', 'events_games') }}
    WHERE event_type = 'game_complete'
      AND DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

-- ── Qualifying cooking events ───────────────────────────────────────────────
cooking_active AS (
    SELECT
        DATE(event_timestamp)  AS event_date,
        subscriber_id,
        'cooking'       AS product,
        platform,
        'cook_mode_start' AS qualifying_event
    FROM {{ source('raw', 'events_cooking') }}
    WHERE event_type = 'cook_mode_start'
      AND DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

-- ── Qualifying athletic events (with quality gate) ──────────────────────────
athletic_active AS (
    SELECT
        DATE(event_timestamp)  AS event_date,
        subscriber_id,
        'athletic'      AS product,
        platform,
        'article_view_60s' AS qualifying_event
    FROM {{ source('raw', 'events_athletic') }}
    WHERE event_type = 'article_view'
      AND time_on_page_sec > 60          -- quality gate: exclude bots and bounces
      AND DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
),

-- ── Union all qualifying events ─────────────────────────────────────────────
all_active AS (
    SELECT * FROM news_active
    UNION ALL
    SELECT * FROM games_active
    UNION ALL
    SELECT * FROM cooking_active
    UNION ALL
    SELECT * FROM athletic_active
),

-- ── Join to subscriber dimension for enrichment ─────────────────────────────
enriched AS (
    SELECT
        a.event_date,
        a.product,
        a.platform,
        a.qualifying_event,
        a.subscriber_id,
        s.plan_type,
        s.bundle_tier,
        s.country,
        s.acquisition_channel
    FROM all_active a
    LEFT JOIN {{ ref('stg_subscribers') }} s
        USING (subscriber_id)
),

-- ── Aggregate to metric grain ────────────────────────────────────────────────
aggregated AS (
    SELECT
        event_date,
        product,
        platform,
        bundle_tier,
        country,
        COUNT(DISTINCT subscriber_id)          AS daily_active_subscribers,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'full_bundle'
              THEN subscriber_id END)          AS daily_active_full_bundle,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'partial_bundle'
              THEN subscriber_id END)          AS daily_active_partial_bundle,
        COUNT(DISTINCT CASE WHEN bundle_tier = 'single_product'
              THEN subscriber_id END)          AS daily_active_single_product
    FROM enriched
    GROUP BY 1,2,3,4,5
),

-- ── Cross-product daily totals (the headline number) ────────────────────────
-- This is a SEPARATE grain: (event_date) only — deduped across all products
-- A subscriber active in Games AND News on the same day counts as 1 DAU, not 2.
cross_product_total AS (
    SELECT
        event_date,
        'ALL_PRODUCTS'                          AS product,
        NULL                                    AS platform,
        NULL                                    AS bundle_tier,
        NULL                                    AS country,
        COUNT(DISTINCT subscriber_id)           AS daily_active_subscribers,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'full_bundle'
              THEN a.subscriber_id END)         AS daily_active_full_bundle,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'partial_bundle'
              THEN a.subscriber_id END)         AS daily_active_partial_bundle,
        COUNT(DISTINCT CASE WHEN s.bundle_tier = 'single_product'
              THEN a.subscriber_id END)         AS daily_active_single_product
    FROM all_active a
    LEFT JOIN {{ ref('stg_subscribers') }} s USING (subscriber_id)
    GROUP BY 1
)

SELECT
    *,
    CURRENT_TIMESTAMP()  AS _model_updated_at,
    '2024.Q1'            AS _metric_version     -- version-pin so downstream can detect definition changes
FROM aggregated

UNION ALL

SELECT
    *,
    CURRENT_TIMESTAMP()  AS _model_updated_at,
    '2024.Q1'            AS _metric_version
FROM cross_product_total

ORDER BY event_date DESC, product
