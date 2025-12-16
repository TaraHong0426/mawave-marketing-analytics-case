{{ config(materialized='table') }}

-- Clean Layer: social_metrics
-- Grain: 1 row per page_id × platform × report_date --PK

with cleaned as (
    select
        cast(page_id as varchar) as page_id,
        trim(page_name) as page_name,
        -- campaign_id는 nullable FK
        case
            when campaign_id is null then null
            else cast(cast(campaign_id as int) as varchar)
        end as campaign_id, --FK - cl_campaigns.campaign_id
        cast(report_date as date) as report_date,
        trim(platform) as platform,
        cast(engaged_users as bigint) as engaged_users,
        cast(new_followers as bigint) as new_followers,
        cast(total_followers as bigint) as total_followers,
        cast(impressions as bigint) as impressions,
        cast(organic_impressions as bigint) as organic_impressions,
        cast(profile_views as bigint) as profile_views,
        cast(website_clicks as bigint) as website_clicks
    from {{ ref('il_social_metrics') }}
)

select *
from cleaned
