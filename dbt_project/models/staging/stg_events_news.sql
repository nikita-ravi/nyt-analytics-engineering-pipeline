-- =============================================================================
-- stg_events_news.sql
-- =============================================================================
-- Staging layer for raw news article engagement events.
-- Handles:
--   • Orphaned events (subscriber_id IS NULL) — filtered out and sent to
--     a quarantine table via a dbt ephemeral model (see _quarantine_news)
--   • Future timestamps (clock skew from mobile SDK) — bounded to CURRENT_TIMESTAMP
--   • Read completion rate definition: article_complete / article_view
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'events_news') }}
),

validated AS (
    SELECT
        event_id,
        subscriber_id,
        event_type,
        article_id,
        section,
        -- Cap wildly-long sessions at 30 min (1800 sec); anything above is
        -- almost certainly an idle tab — a documented data quality issue
        LEAST(CAST(time_on_page_sec AS FLOAT64), 1800.0)  AS time_on_page_sec,
        CAST(event_timestamp AS TIMESTAMP)                  AS event_timestamp,
        -- Clamp future-backdated records to current time
        CASE WHEN CAST(event_timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP()
             THEN CURRENT_TIMESTAMP()
             ELSE CAST(event_timestamp AS TIMESTAMP) END    AS event_timestamp_clean,
        DATE(CAST(event_timestamp AS TIMESTAMP))            AS event_date,
        platform,
        referrer,

        -- DQ flag columns — surfaced in monitoring dashboard
        subscriber_id IS NULL                               AS is_orphaned_event,
        CAST(event_timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP()
                                                            AS has_future_timestamp,
        time_on_page_sec IS NULL                            AS is_missing_dwell_time
    FROM source
),

clean AS (
    SELECT * FROM validated
    WHERE NOT is_orphaned_event        -- quarantine handled separately
)

SELECT * FROM clean
