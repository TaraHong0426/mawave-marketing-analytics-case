{{ config(materialized='table') }}

-- Clean Layer: campaigns
-- Grain: 1 row per campaign_id

with cleaned as (

    select
        cast(campaign_id as varchar) as campaign_id, --PK
        cast(account_id as varchar) as account_id, --FK - cl_clients.client_id (accounts = clients 1:1 구조)
        trim(account_name) as account_name,
        trim(campaign_name) as campaign_name,
        trim(platform) as platform,
        start_date,
        -- start_date: timestamp
        cast(start_date as timestamp) as start_timestamp,
        nullif(upper(trim(campaign_status)), '') as campaign_status,
        cast(daily_budget_eur as double) as daily_budget_eur
    from {{ ref('il_campaigns') }}
)

select *
from cleaned
