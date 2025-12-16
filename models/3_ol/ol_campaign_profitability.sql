{{ config(materialized='table') }}

-- Operational Layer: Campaign Profitability
-- Grain: 1 row per client_id × campaign_id × platform × report_date × attribution_window

-- Keys:
--   - campaign_id (FK → cl_campaigns.campaign_id)
--   - client_id (FK → cl_clients.client_id)

-- Sources:
--   - ol_unified_ad_metrics (campaign-level revenue & media spend)
--   - ol_client_projects_with_time (client/project-level internal time & cost)
--       -- no campaign_id, so internal cost is estimated via allocation

-- Intended Profitability calc:
--   - full_total_cost_eur = ad_spend_eur + internal_cost_eur
--   - full_profit_eur = revenue_eur - full_total_cost_eur
--   - full margins:
--       * margin_on_revenue_full = full_profit_eur / revenue_eur
--       * margin_on_cost_full = full_profit_eur / full_total_cost_eur

--   Additionally, we expose "media-only" metrics that do not rely on internal cost:
--   - media_only_total_cost_eur = ad_spend_eur
--   - media_only_profit_eur = revenue_eur - ad_spend_eur
--   - media-only margins:
--       * media_only_margin_on_revenue = media_only_profit_eur / revenue_eur
--       * media_only_margin_on_cost = media_only_profit_eur / media_only_total_cost_eur


with campaign_perf as (
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
        spend_eur as ad_spend_eur,
        impressions,
        clicks,
        conversions,
        revenue_eur,
        cpm_calc,
        ctr_calc,
        cvr_calc,
        cpc_calc,
        cpa_calc,
        roas_calc
    from {{ ref('ol_unified_ad_metrics') }}
    --where attribution_window = '7d_click' -- sample
),

client_time as (
    -- client × report_date : internal cost per day
    select
        client_id,
        report_date,
        sum(total_cost_eur) as client_internal_cost_eur
    from {{ ref('ol_client_projects_with_time') }}
    group by client_id, report_date
),

client_spend as (
    -- client × report_date : media spend per day
    select
        client_id,
        report_date,
        sum(ad_spend_eur) as client_total_spend_eur
    from campaign_perf
    group by client_id, report_date
),

client_time_total as (
    -- client total internal cost across full observed period
    select
        client_id,
        sum(total_cost_eur) as client_total_internal_cost_eur
    from {{ ref('ol_client_projects_with_time') }}
    group by client_id
),

client_spend_total as (
    -- client total media spend across full observed period
    select
        client_id,
        sum(ad_spend_eur) as client_lifetime_spend_eur
    from campaign_perf
    group by client_id
),

joined as (
    select
        cp.*,
        ct.client_internal_cost_eur,
        cs.client_total_spend_eur,
        ctt.client_total_internal_cost_eur,
        cst.client_lifetime_spend_eur,

        -- spend ratio - internal cost allocation (daily)
        case
            when cs.client_total_spend_eur > 0 then cp.ad_spend_eur / cs.client_total_spend_eur
            else null
        end as spend_share_in_client_day
    from campaign_perf cp
    -- client + report_date based join for day-level allocation
    left join client_time ct on cp.client_id = ct.client_id and cp.report_date = ct.report_date
    left join client_spend cs on cp.client_id = cs.client_id and cp.report_date = cs.report_date
    -- client-level totals for fallback allocation
    left join client_time_total ctt on cp.client_id = ctt.client_id
    left join client_spend_total cst on cp.client_id = cst.client_id
),

allocated as (
    select
        j.*,
        -- Allocate internal cost in two steps:
        -- 1) Prefer client × day allocation by spend share (if day-level data exists)
        -- 2) Fallback to client-level lifetime allocation by lifetime spend share
        case
            -- 1st priority: daily allocation (client × day)
            when j.client_internal_cost_eur is not null and j.spend_share_in_client_day is not null
            then j.client_internal_cost_eur * j.spend_share_in_client_day

            -- 2nd priority: lifetime allocation (client-level)
            when j.client_internal_cost_eur is null and j.client_total_internal_cost_eur is not null and j.client_lifetime_spend_eur > 0
            then j.client_total_internal_cost_eur * (j.ad_spend_eur / j.client_lifetime_spend_eur)
            else null
        end as internal_cost_eur
    from joined j
),

profitability as (
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
        ad_spend_eur,
        impressions,
        clicks,
        conversions,
        revenue_eur,
        cpm_calc,
        ctr_calc,
        cvr_calc,
        cpc_calc,
        cpa_calc,
        roas_calc,
        client_internal_cost_eur,
        client_total_spend_eur,
        client_total_internal_cost_eur,
        client_lifetime_spend_eur,
        spend_share_in_client_day,
        internal_cost_eur,

        -- FULL COST (media + allocated internal cost, may be equal to media-only if internal_cost_eur is NULL)
        case
            when internal_cost_eur is not null then ad_spend_eur + internal_cost_eur
            else null
        end as full_total_cost_eur,
        case
            when revenue_eur is not null and internal_cost_eur is not null then revenue_eur - (ad_spend_eur + internal_cost_eur)
            else null
        end as full_profit_eur,

        -- MEDIA-ONLY COST & PROFIT (does not depend on internal_cost_eur)
        ad_spend_eur as media_only_total_cost_eur,

        case
            when revenue_eur is not null and ad_spend_eur is not null
            then round(revenue_eur - ad_spend_eur, 2)
            else null
        end as media_only_profit_eur
    from allocated
),

margins as (
    select
        *,
        -- full margins (may coincide with media-only margins if internal_cost_eur is NULL)
        case
            when revenue_eur > 0 and full_profit_eur is not null
            then full_profit_eur / revenue_eur
            else null
        end as margin_on_revenue_full,

        case
            when full_total_cost_eur > 0 and full_profit_eur is not null
            then full_profit_eur / full_total_cost_eur
            else null
        end as margin_on_cost_full,

        -- media-only margins
        case
            when revenue_eur > 0 and media_only_profit_eur is not null
            then round(media_only_profit_eur / revenue_eur, 2)
            else null
        end as media_only_margin_on_revenue,

        case
            when media_only_total_cost_eur > 0 and media_only_profit_eur is not null
            then round(media_only_profit_eur / media_only_total_cost_eur, 2)
            else null
        end as media_only_margin_on_cost
    from profitability
)

select *
from margins
