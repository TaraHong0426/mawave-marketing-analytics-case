{{ config(materialized='view') }}

-- Integration Layer view for ad performance metrics
-- Grain: 1 row per campaign_id, report_date, attribution_window

select
    *
from {{ source('raw', 'ad_metrics') }}
