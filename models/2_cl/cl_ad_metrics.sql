{{ config(materialized='table') }}

-- Clean Layer: ad_metrics
-- Grain: 1 row per campaign_id × platform × report_date × attribution_window --PK

with cleaned as (
    select
        cast(campaign_id as varchar) as campaign_id, --FK - cl_campaigns.campaign_id
        cast(client_id as varchar) as client_id, --FK - cl_clients.client_id
        trim(client_name) as client_name,
        trim(platform) as platform,
        cast(report_date as date) as report_date,
        trim(attribution_window) as attribution_window,

        -- spend & performance metrics numeric cast
        cast(spend_eur as double) as spend_eur,
        cast(impressions as bigint) as impressions, -- safe w integer for duckdb
        cast(clicks as bigint) as clicks,
        cast(conversions as bigint) as conversions,
        cast(cpm as double) as cpm,
        cast(ctr as double) as ctr,
        cast(cvr as double) as cvr,
        cast(cpc as double) as cpc,
        cast(cpa as double) as cpa,
        cast(revenue_eur as double) as revenue_eur,
        cast(roas as double) as roas
    from {{ ref('il_ad_metrics') }}
)

select *
from cleaned
