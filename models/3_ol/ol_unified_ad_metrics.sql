{{ config(materialized='table') }}

-- Operational Layer: Unified Ad Metrics
-- Grain:
--   1 row per client_id × campaign_id × platform × report_date × attribution_window

-- Keys:
--   - campaign_id (FK - cl_campaigns.campaign_id)
--   - client_id (FK - cl_clients.client_id)

-- Sources:
--   - cl_ad_metrics
--   - cl_campaigns
--   - cl_clients

with base as (
    select
        m.campaign_id,
        m.client_id,
        c.campaign_name,
        c.platform,
        m.report_date,
        m.attribution_window,
        cl.client_name,
        cl.primary_industry,
        cl.country,
        c.campaign_status,
        c.daily_budget_eur,
        m.spend_eur,
        m.impressions,
        m.clicks,
        m.conversions,
        m.revenue_eur
    from {{ ref('cl_ad_metrics') }} as m
    left join {{ ref('cl_campaigns') }} as c on m.campaign_id = c.campaign_id
    left join {{ ref('cl_clients') }} as cl on m.client_id = cl.client_id
),

-- agg main metricsaa
aggregated as (
    select
        client_id,
        client_name,
        primary_industry,
        country,
        campaign_id,
        campaign_name,
        platform,
        report_date,
        attribution_window,
        campaign_status,
        daily_budget_eur,

        sum(spend_eur) as spend_eur,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(revenue_eur) as revenue_eur
    from base
    group by
        client_id,
        client_name,
        primary_industry,
        country,
        campaign_id,
        campaign_name,
        platform,
        report_date,
        attribution_window,
        campaign_status,
        daily_budget_eur
),

-- agg KPI with sum metrics (clear)
kpis as (
    select
        *,
        case
            when impressions > 0 then spend_eur * 1000.0 / impressions else null
        end as cpm_calc, -- Cost per mille (thousand impressions)
        case
            when impressions > 0 then clicks * 1.0 / impressions else null
        end as ctr_calc, -- Click-through rate (%)
        case
            when clicks > 0 then conversions * 1.0 / clicks else null
        end as cvr_calc, -- Conversion rate (%)
        case
            when clicks > 0 then spend_eur / clicks else null
        end as cpc_calc, -- Cost per click
        case
            when conversions > 0 then spend_eur / conversions else null
        end as cpa_calc, -- Cost per acquisition/conversion
        case
            when spend_eur > 0 then revenue_eur / spend_eur else null
        end as roas_calc -- Return on ad spend (revenue / spend)
    from aggregated
)

select *
from kpis
