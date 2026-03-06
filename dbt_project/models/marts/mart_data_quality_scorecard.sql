-- =============================================================================
-- mart_data_quality_scorecard.sql
-- =============================================================================
-- ENTERPRISE MART — Data Quality Monitoring
-- =============================================================================
-- Grain: one row per dataset per run_date
-- 
-- Purpose: Proactively surfaces data quality issues before they propagate
-- to downstream marts and executive dashboards. Core deliverable of the
-- Analytics Engineering DQ Framework.
--
-- Dimensions monitored per dataset:
--   • Completeness   — % of non-null values in key columns
--   • Uniqueness     — % of unique primary keys
--   • Timeliness     — % of records with valid (non-future) timestamps
--   • Referential    — % of events with a matching subscriber record
--   • Validity       — % of records passing domain-specific business rules
--
-- DQ Score formula:
--   weighted_avg(completeness=0.30, uniqueness=0.20, timeliness=0.15,
--                referential=0.20, validity=0.15)
-- =============================================================================

WITH run_date AS (SELECT CURRENT_DATE() AS today),

-- ── Subscriber DQ ──────────────────────────────────────────────────────────
subs_raw AS (
    SELECT * FROM {{ source('raw', 'subscribers') }}
),

subs_dq AS (
    SELECT
        'subscribers'                                               AS dataset_name,
        'subscriber_domain'                                         AS data_domain,
        rd.today                                                    AS run_date,
        COUNT(*)                                                    AS total_records,

        -- Completeness
        SAFE_DIVIDE(COUNTIF(subscriber_id IS NOT NULL), COUNT(*))  AS completeness_subscriber_id,
        SAFE_DIVIDE(COUNTIF(plan_type IS NOT NULL), COUNT(*))      AS completeness_plan_type,
        SAFE_DIVIDE(COUNTIF(subscription_start_date IS NOT NULL), COUNT(*))
                                                                    AS completeness_start_date,

        -- Uniqueness (ratio of distinct IDs to total rows)
        SAFE_DIVIDE(COUNT(DISTINCT subscriber_id), COUNT(*))        AS uniqueness_score,

        -- Timeliness: records with start_date not in the future
        SAFE_DIVIDE(
            COUNTIF(CAST(subscription_start_date AS DATE) <= rd.today),
            COUNT(*)
        )                                                           AS timeliness_score,

        -- Validity: plan_type in allowed set
        SAFE_DIVIDE(
            COUNTIF(plan_type IN ('bundle_all','bundle_3','news_only',
                                  'games_only','cooking_only','athletic_only')),
            COUNT(*)
        )                                                           AS validity_score,

        -- Referential: N/A for root entity
        1.0                                                         AS referential_integrity_score

    FROM subs_raw, run_date rd
    GROUP BY 1,2,3
),

-- ── News events DQ ────────────────────────────────────────────────────────
news_raw AS (SELECT * FROM {{ source('raw', 'events_news') }}),
subs_clean AS (SELECT DISTINCT subscriber_id FROM {{ ref('stg_subscribers') }}),

news_dq AS (
    SELECT
        'events_news'                                               AS dataset_name,
        'news_domain'                                               AS data_domain,
        rd.today                                                    AS run_date,
        COUNT(*)                                                    AS total_records,

        SAFE_DIVIDE(COUNTIF(event_id IS NOT NULL), COUNT(*))        AS completeness_subscriber_id,
        SAFE_DIVIDE(COUNTIF(event_type IS NOT NULL), COUNT(*))      AS completeness_plan_type,
        SAFE_DIVIDE(COUNTIF(event_timestamp IS NOT NULL), COUNT(*)) AS completeness_start_date,

        SAFE_DIVIDE(COUNT(DISTINCT event_id), COUNT(*))             AS uniqueness_score,

        SAFE_DIVIDE(
            COUNTIF(CAST(event_timestamp AS TIMESTAMP) <= CURRENT_TIMESTAMP()),
            COUNT(*)
        )                                                           AS timeliness_score,

        SAFE_DIVIDE(
            COUNTIF(event_type IN ('article_view','article_complete','share','save','comment')),
            COUNT(*)
        )                                                           AS validity_score,

        -- Referential: % of events with a valid subscriber_id in master
        SAFE_DIVIDE(
            (SELECT COUNT(*) FROM news_raw n
             JOIN subs_clean s ON n.subscriber_id = s.subscriber_id),
            COUNT(*)
        )                                                           AS referential_integrity_score

    FROM news_raw, run_date rd
    GROUP BY 1,2,3
),

-- ── Union all domain DQ checks ────────────────────────────────────────────
unioned AS (
    SELECT * FROM subs_dq
    UNION ALL
    SELECT * FROM news_dq
),

-- ── Compute composite DQ score ────────────────────────────────────────────
scored AS (
    SELECT
        *,
        -- Weighted composite score
        ROUND(
            (completeness_subscriber_id * 0.10
             + completeness_plan_type   * 0.10
             + completeness_start_date  * 0.10
             + uniqueness_score         * 0.20
             + timeliness_score         * 0.15
             + referential_integrity_score * 0.20
             + validity_score           * 0.15
            ) * 100, 2
        )                                  AS dq_score,

        CASE
            WHEN (completeness_subscriber_id * 0.10
                  + completeness_plan_type   * 0.10
                  + completeness_start_date  * 0.10
                  + uniqueness_score         * 0.20
                  + timeliness_score         * 0.15
                  + referential_integrity_score * 0.20
                  + validity_score           * 0.15) >= 0.95  THEN 'PASS'
            WHEN (completeness_subscriber_id * 0.10
                  + completeness_plan_type   * 0.10
                  + completeness_start_date  * 0.10
                  + uniqueness_score         * 0.20
                  + timeliness_score         * 0.15
                  + referential_integrity_score * 0.20
                  + validity_score           * 0.15) >= 0.85  THEN 'WARN'
            ELSE 'FAIL'
        END                                AS dq_status
    FROM unioned
)

SELECT * FROM scored
ORDER BY dq_score ASC  -- Worst datasets first for triage prioritization
