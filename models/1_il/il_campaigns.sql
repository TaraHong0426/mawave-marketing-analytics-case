{{ config(materialized='view') }}

-- Integration Layer view for campaigns
-- Grain: 1 row per campaign_id

select
    *
from {{ source('raw', 'campaigns') }}
