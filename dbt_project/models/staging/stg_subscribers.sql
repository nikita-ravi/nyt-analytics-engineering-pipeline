-- =============================================================================
-- stg_subscribers.sql
-- =============================================================================
-- Staging layer for the subscriber domain.
-- Primary responsibilities:
--   1. Deduplicate upstream CRM records (known bug: duplicate inserts on plan
--      upgrades — subscriber_id + created_at composite dedup)
--   2. Cast raw string dates to DATE types
--   3. Derive product_access flags from plan_type (single source of truth
--      so downstream marts never re-parse plan strings)
--   4. Apply standard field naming conventions
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'subscribers') }}
),

deduped AS (
    -- Dedup strategy: keep the FIRST record per subscriber_id, ordered by
    -- created_at ascending. This guards against upstream CRM double-writes
    -- observed during plan upgrade events.
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY subscriber_id
                ORDER BY created_at ASC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
),

typed AS (
    SELECT
        subscriber_id,
        email_domain,
        plan_type,
        CAST(subscription_start_date AS DATE)             AS subscription_start_date,
        CAST(subscription_end_date   AS DATE)             AS subscription_end_date,
        is_churned,
        acquisition_channel,
        country,
        device_primary,
        CAST(created_at AS TIMESTAMP)                     AS created_at,

        -- ── Product access flags (derived from plan) ──────────────────────
        -- These replace ad-hoc CASE logic scattered across downstream models
        CASE WHEN plan_type IN ('bundle_all','bundle_3','news_only')
             THEN TRUE ELSE FALSE END                     AS has_news_access,

        CASE WHEN plan_type IN ('bundle_all','bundle_3','games_only')
             THEN TRUE ELSE FALSE END                     AS has_games_access,

        CASE WHEN plan_type IN ('bundle_all','bundle_3','cooking_only')
             THEN TRUE ELSE FALSE END                     AS has_cooking_access,

        CASE WHEN plan_type IN ('bundle_all','athletic_only')
             THEN TRUE ELSE FALSE END                     AS has_athletic_access,

        CASE WHEN plan_type = 'bundle_all'
             THEN TRUE ELSE FALSE END                     AS has_wirecutter_access,

        -- ── Bundle tier classification (for churn risk bucketing) ─────────
        CASE
            WHEN plan_type = 'bundle_all'               THEN 'full_bundle'
            WHEN plan_type = 'bundle_3'                 THEN 'partial_bundle'
            ELSE 'single_product'
        END                                               AS bundle_tier,

        -- ── Tenure computation ────────────────────────────────────────────
        DATE_DIFF(
            COALESCE(CAST(subscription_end_date AS DATE), CURRENT_DATE()),
            CAST(subscription_start_date AS DATE),
            DAY
        )                                                 AS tenure_days
    FROM deduped
)

SELECT * FROM typed
