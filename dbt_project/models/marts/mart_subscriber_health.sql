-- =============================================================================
-- mart_subscriber_health.sql
-- =============================================================================
-- ENTERPRISE MART — Subscriber Domain Health
-- =============================================================================
-- Grain: one row per subscriber per day (or just latest snapshot if daily
--        snapshots not yet implemented — see TODO)
-- 
-- Business purpose:
--   Provides the single authoritative view of subscriber health, combining:
--     • Subscription status and plan metadata
--     • Cross-product engagement signals (all 5 products)
--     • Churn risk score (deterministic rule-based v1 — ML v2 in roadmap)
--     • Engagement velocity (trailing 7-day vs 30-day comparison)
--
-- Primary consumers:
--   • Executive churn dashboard
--   • Retention intervention workflows (triggers email campaigns)
--   • Data Science team (feature store for churn ML model)
--   • Finance reporting (MRR, Net Revenue Retention)
-- =============================================================================

WITH subscribers AS (
    SELECT * FROM {{ ref('stg_subscribers') }}
),

-- ── News engagement summary ───────────────────────────────────────────────
news_agg AS (
    SELECT
        subscriber_id,
        COUNT(DISTINCT event_date)                                      AS news_active_days,
        COUNT(CASE WHEN event_type = 'article_view' THEN 1 END)         AS news_article_views,
        COUNT(CASE WHEN event_type = 'article_complete' THEN 1 END)     AS news_articles_completed,
        SAFE_DIVIDE(
            COUNT(CASE WHEN event_type = 'article_complete' THEN 1 END),
            NULLIF(COUNT(CASE WHEN event_type = 'article_view' THEN 1 END), 0)
        )                                                               AS news_completion_rate,
        AVG(time_on_page_sec)                                           AS news_avg_dwell_sec,
        MAX(event_date)                                                 AS news_last_active_date
    FROM {{ ref('stg_events_news') }}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
    GROUP BY 1
),

-- ── Games engagement summary ──────────────────────────────────────────────
games_agg AS (
    SELECT
        subscriber_id,
        COUNT(DISTINCT event_date)                                       AS games_active_days,
        COUNT(CASE WHEN event_type = 'game_complete' THEN 1 END)         AS games_completed,
        COUNT(DISTINCT game_name)                                        AS games_distinct_games_played,
        MAX(streak_days)                                                 AS games_max_streak,
        MAX(event_date)                                                  AS games_last_active_date
    FROM {{ source('raw', 'events_games') }}
    WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
    GROUP BY 1
),

-- ── Cooking engagement summary ────────────────────────────────────────────
cooking_agg AS (
    SELECT
        subscriber_id,
        COUNT(DISTINCT event_date)                                       AS cooking_active_days,
        COUNT(CASE WHEN event_type = 'cook_mode_complete' THEN 1 END)    AS recipes_cooked,
        COUNT(CASE WHEN event_type = 'save' THEN 1 END)                  AS recipes_saved,
        MAX(event_date)                                                  AS cooking_last_active_date
    FROM {{ source('raw', 'events_cooking') }}
    WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
    GROUP BY 1
),

-- ── Athletic engagement summary ───────────────────────────────────────────
athletic_agg AS (
    SELECT
        subscriber_id,
        COUNT(DISTINCT event_date)                                       AS athletic_active_days,
        COUNT(CASE WHEN event_type = 'article_view' THEN 1 END)          AS athletic_article_views,
        MAX(event_date)                                                  AS athletic_last_active_date
    FROM {{ source('raw', 'events_athletic') }}
    WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
    GROUP BY 1
),

-- ── Compute cross-product breadth ─────────────────────────────────────────
enriched AS (
    SELECT
        s.*,

        -- News
        COALESCE(n.news_active_days, 0)           AS news_active_days,
        COALESCE(n.news_article_views, 0)         AS news_article_views,
        COALESCE(n.news_articles_completed, 0)    AS news_articles_completed,
        COALESCE(n.news_completion_rate, 0)       AS news_completion_rate,
        n.news_avg_dwell_sec,
        n.news_last_active_date,

        -- Games
        COALESCE(g.games_active_days, 0)          AS games_active_days,
        COALESCE(g.games_completed, 0)            AS games_completed,
        COALESCE(g.games_distinct_games_played, 0)AS games_distinct_played,
        COALESCE(g.games_max_streak, 0)           AS games_max_streak,
        g.games_last_active_date,

        -- Cooking
        COALESCE(c.cooking_active_days, 0)        AS cooking_active_days,
        COALESCE(c.recipes_cooked, 0)             AS recipes_cooked,
        COALESCE(c.recipes_saved, 0)              AS recipes_saved,
        c.cooking_last_active_date,

        -- Athletic
        COALESCE(a.athletic_active_days, 0)       AS athletic_active_days,
        COALESCE(a.athletic_article_views, 0)     AS athletic_article_views,
        a.athletic_last_active_date,

        -- ── Cross-product breadth score ───────────────────────────────────
        -- Number of distinct products a subscriber engaged with in the window
        (CASE WHEN COALESCE(n.news_active_days, 0) > 0 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(g.games_active_days, 0) > 0 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(c.cooking_active_days, 0) > 0 THEN 1 ELSE 0 END
         + CASE WHEN COALESCE(a.athletic_active_days, 0) > 0 THEN 1 ELSE 0 END
        )                                         AS products_engaged_count,

        -- ── Recency: days since last active across ANY product ────────────
        LEAST(
            DATE_DIFF(CURRENT_DATE(), COALESCE(n.news_last_active_date, DATE('1970-01-01')), DAY),
            DATE_DIFF(CURRENT_DATE(), COALESCE(g.games_last_active_date, DATE('1970-01-01')), DAY),
            DATE_DIFF(CURRENT_DATE(), COALESCE(c.cooking_last_active_date, DATE('1970-01-01')), DAY),
            DATE_DIFF(CURRENT_DATE(), COALESCE(a.athletic_last_active_date, DATE('1970-01-01')), DAY)
        )                                         AS days_since_last_activity

    FROM subscribers s
    LEFT JOIN news_agg    n USING (subscriber_id)
    LEFT JOIN games_agg   g USING (subscriber_id)
    LEFT JOIN cooking_agg c USING (subscriber_id)
    LEFT JOIN athletic_agg a USING (subscriber_id)
),

-- ── Churn risk scoring (rule-based v1) ────────────────────────────────────
-- TODO: Replace with ML feature lookup once Data Science ships model v2
scored AS (
    SELECT
        *,
        CASE
            -- High risk: dormant 30+ days AND single_product
            WHEN days_since_last_activity >= 30
             AND bundle_tier = 'single_product'                        THEN 'high'
            -- High risk: dormant 30+ days on partial bundle
            WHEN days_since_last_activity >= 30
             AND bundle_tier = 'partial_bundle'                        THEN 'high'
            -- Medium risk: dormant 14-29 days
            WHEN days_since_last_activity BETWEEN 14 AND 29           THEN 'medium'
            -- Medium risk: only 1 product engaged despite bundle access
            WHEN products_engaged_count = 1
             AND bundle_tier IN ('full_bundle','partial_bundle')       THEN 'medium'
            -- Low risk: multi-product engaged recent activity
            ELSE                                                          'low'
        END                                                            AS churn_risk_tier,

        -- Revenue contribution proxy (simplified MRR weight)
        CASE bundle_tier
            WHEN 'full_bundle'     THEN 20.00
            WHEN 'partial_bundle'  THEN 14.00
            ELSE                        5.00
        END                                                            AS mrr_estimate_usd

    FROM enriched
)

SELECT * FROM scored
