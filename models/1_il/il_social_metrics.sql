{{ config(materialized='view') }}

-- Integration Layer view for social media metrics
-- Grain: 1 row per client_id, platform, report_date

select
    *
from {{ source('raw', 'social_metrics') }}
