{{ config(materialized='table') }}

-- Business Layer: Monthly Campaign Cohorts
-- Grain: 1 row per cohort_month × client_id × platform × attribution_window
-- Sources:
--   - ol_campaign_profitability (campaign × day, media-only profitability)
--   - ol_client_projects_with_time (client × project × day, internal time & cost)
-- Purpose:
--   Monthly cohort view combining:
--     * Paid media performance
--     * Media-only profitability
--     * Internal resource cost & hours (monthly)
--     * Profitability after internal cost (project margin)

-- NOTE:
--   Internal cost is only available at client level, not per platform/window.
--   When joined, monthly internal cost is repeated across platforms/windows for a client.
--   This is the same interpretation as in bl_client_performance_dashboard.

with campaign_monthly as (
    select
        client_id,
        client_name,
        primary_industry,
        country,
        platform,
        attribution_window,

        -- Cohort month
        date_trunc('month', cast(report_date as date)) as cohort_month,

        -- Campaign within the month
        count(distinct campaign_id) as num_campaigns,
        min(report_date) as first_activity_date_in_month,
        max(report_date) as last_activity_date_in_month,

        -- Core performance volumes
        sum(impressions) as monthly_impressions,
        sum(clicks) as monthly_clicks,
        sum(conversions) as monthly_conversions,

        -- Spend & revenue (media)
        round(sum(ad_spend_eur), 2) as monthly_ad_spend_eur,
        round(sum(revenue_eur), 2) as monthly_revenue_eur,

        -- Media-only profitability (from OL model)
        round(sum(media_only_total_cost_eur),  2) as media_only_total_cost_eur,
        round(sum(media_only_profit_eur), 2) as media_only_profit_eur
    from {{ ref('ol_campaign_profitability') }}
    group by
        client_id,
        client_name,
        primary_industry,
        country,
        platform,
        attribution_window,
        date_trunc('month', cast(report_date as date))
),

client_internal_monthly as (
    -- Monthly internal time & cost by client (all projects)
    select
        client_id,
        date_trunc('month', report_date) as cohort_month,
        round(sum(total_hours), 2) as monthly_total_hours_all_projects,
        round(sum(total_cost_eur), 2) as monthly_internal_cost_eur,
        round(sum(productive_hours), 2) as monthly_productive_hours,
        round(sum(productive_cost_eur), 2) as monthly_productive_internal_cost_eur
    from {{ ref('ol_client_projects_with_time') }}
    group by
        client_id,
        date_trunc('month', report_date)
),

joined as (
    select
        cm.client_id,
        cm.client_name,
        cm.primary_industry,
        cm.country,
        cm.platform,
        cm.attribution_window,
        cm.cohort_month,
        cm.num_campaigns,
        cm.first_activity_date_in_month,
        cm.last_activity_date_in_month,
        cm.monthly_impressions,
        cm.monthly_clicks,
        cm.monthly_conversions,
        cm.monthly_ad_spend_eur,
        cm.monthly_revenue_eur,
        cm.media_only_total_cost_eur,
        cm.media_only_profit_eur,
        cim.monthly_total_hours_all_projects,
        cim.monthly_internal_cost_eur,
        cim.monthly_productive_hours,
        cim.monthly_productive_internal_cost_eur
    from campaign_monthly cm
    left join client_internal_monthly cim on cm.client_id   = cim.client_id and cm.cohort_month = cim.cohort_month
),

final as (
    select
        client_id,
        client_name,
        primary_industry,
        country,
        platform,
        attribution_window,
        cohort_month,

        -- campaign footprint
        num_campaigns,
        first_activity_date_in_month,
        last_activity_date_in_month,

        -- monthly volumes
        monthly_impressions,
        monthly_clicks,
        monthly_conversions,
        monthly_ad_spend_eur,
        monthly_revenue_eur,

        -- media-only profitability (monthly, from OL)
        media_only_total_cost_eur,
        media_only_profit_eur,

        -- internal resource usage & cost (monthly)
        monthly_total_hours_all_projects,
        monthly_internal_cost_eur,
        monthly_productive_hours,
        monthly_productive_internal_cost_eur,

        -- Campaign Performance KPIs (monthly, blended)
        case
            when monthly_impressions > 0 then round(monthly_clicks * 1.0 / monthly_impressions, 4)
        end as monthly_ctr,
        case
            when monthly_clicks > 0 then round(monthly_conversions * 1.0 / monthly_clicks, 4)
        end as monthly_cvr,
        case
            when monthly_clicks > 0 then round(monthly_ad_spend_eur / monthly_clicks, 2)
        end as monthly_cpc_eur,
        case
            when monthly_conversions > 0 then round(monthly_ad_spend_eur / monthly_conversions, 2)
        end as monthly_cpa_eur,
        case
            when monthly_ad_spend_eur > 0 then round(monthly_revenue_eur / monthly_ad_spend_eur, 2)
        end as monthly_roas,

        -- Media-only margins (monthly)
        case
            when monthly_revenue_eur > 0 and media_only_profit_eur is not null
            then round(media_only_profit_eur * 1.0 / monthly_revenue_eur, 2)
        end as monthly_media_only_margin_on_revenue,
        case
            when media_only_total_cost_eur > 0 and media_only_profit_eur is not null
            then round(media_only_profit_eur * 1.0 / media_only_total_cost_eur, 2)
        end as monthly_media_only_margin_on_cost,

        -- Resource Allocation KPIs (monthly)
        case
            when monthly_total_hours_all_projects > 0
            then round(monthly_productive_hours * 1.0 / monthly_total_hours_all_projects, 4)
        end as monthly_utilization_rate_billable_hours,
        case
            when monthly_total_hours_all_projects > 0
            then round(monthly_internal_cost_eur * 1.0 / monthly_total_hours_all_projects, 2)
        end as monthly_internal_cost_per_hour_eur,

        -- Profitability after internal cost (monthly)
        case
            when media_only_profit_eur is not null and monthly_internal_cost_eur is not null
            then round(media_only_profit_eur - monthly_internal_cost_eur, 2)
        end as monthly_net_profit_after_internal_eur,

        case
            when monthly_revenue_eur > 0 and media_only_profit_eur is not null and monthly_internal_cost_eur is not null
            then round((media_only_profit_eur - monthly_internal_cost_eur) / monthly_revenue_eur, 2)
        end as monthly_project_margin_after_internal
    from joined
)

select * from final
order by cohort_month, client_id, platform, attribution_window
