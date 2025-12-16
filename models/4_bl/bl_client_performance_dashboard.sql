{{ config(materialized='table') }}

-- Business Layer: Client Performance Dashboard
-- Grain: 1 row per client_id × platform × attribution_window
-- Sources:
--   - ol_campaign_profitability (campaign/day level, media-only/full profit calc)
--   - ol_client_projects_with_time (client/project level internal time & cost)
--   - social_metrics (daily organic social performance)
-- Business purpose:
--   - Provide a single, consistent client-level view combining:
--       * Paid media performance (spend, revenue, conversions, efficiency KPIs)
--       * Media-only profitability (from OL profitability model)
--       * Internal resource usage and cost
--       * Organic/social performance
--   - This model is the main input for the “Client Performance Dashboard” in the BI layer.


with campaign_perf as (
    -- 1) Aggregate campaign/day level profitability to client level
    select distinct
        client_id,
        client_name,
        primary_industry,
        platform,
        attribution_window,
        count(distinct campaign_id) as num_campaigns,
        min(report_date) as first_activity_date,
        max(report_date) as last_activity_date,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions,
        -- Spend & revenue
        round(sum(ad_spend_eur), 2) as total_ad_spend_eur,
        round(sum(revenue_eur), 2) as total_revenue_eur,

        -- Client-level blended performance KPIs
        case
            when sum(impressions) > 0 then round(sum(clicks) * 1.0 / sum(impressions), 2)
            else null end as blended_ctr,
        case
            when sum(clicks) > 0 then round(sum(conversions) * 1.0 / sum(clicks), 2)
            else null end as blended_cvr,
        case
            when sum(clicks) > 0 then round(sum(ad_spend_eur) / sum(clicks), 2)
            else null end as blended_cpc_eur,
        case
            when sum(conversions) > 0 then round(sum(ad_spend_eur) / sum(conversions), 2)
            else null end as blended_cpa_eur,
        case
            when sum(ad_spend_eur) > 0 then round(sum(revenue_eur) / sum(ad_spend_eur), 2)
            else null end as blended_roas,

        -- Media-only cost & profit aggregated to client level
        round(sum(media_only_total_cost_eur), 2) as media_only_total_cost_eur_client,
        round(sum(media_only_profit_eur), 2) as media_only_profit_eur_client,

        -- Client-level media-only margins
        case
            when sum(revenue_eur) > 0 then round(sum(media_only_profit_eur) / sum(revenue_eur), 2)
            else null end as media_only_margin_on_revenue_client,
        case
            when sum(media_only_total_cost_eur) > 0 then round(sum(media_only_profit_eur) / sum(media_only_total_cost_eur), 2)
            else null end as media_only_margin_on_cost_client
    from {{ ref('ol_campaign_profitability') }}
    group by
        client_id, client_name, primary_industry, platform, attribution_window
),

client_internal as (
    -- 2) Aggregate internal resource usage & cost by client
    --    Column names may need to be aligned with the actual OL model.
    select distinct
        client_id,
        round(sum(total_hours), 2) as total_hours_all_projects,
        round(sum(total_cost_eur), 2) as total_internal_cost_eur,
        round(sum(productive_hours), 2) as productive_hours,
        round(sum(productive_cost_eur), 2) as productive_internal_cost_eur
    from {{ ref('ol_client_projects_with_time') }}
    group by client_id
),

client_social as (
    -- 3) Aggregate social/organic performance by client
        with social_with_client as (
        select
            cli.client_id,
            sm.platform,
            sm.report_date,
            sm.new_followers,
            sm.engaged_users,
            sm.website_clicks,
            sm.total_followers
        from {{ ref('cl_social_metrics') }} sm 
        left join {{ ref('cl_campaigns') }} cam on sm.campaign_id = cam.campaign_id
        left join {{ ref('cl_clients') }} cli on cam.account_id = cli.client_id
        -- Only keep rows where we can reliably assign a client_id
        where cli.client_id is not null
    )
    select distinct
        client_id,
        count(distinct report_date) as n_report_days_social,

        -- channel-specific report days
        count(distinct case when platform = 'Facebook'  then report_date end) as n_report_days_facebook,
        count(distinct case when platform = 'Instagram' then report_date end) as n_report_days_instagram,

        -- Facebook metrics (total + daily average)
        sum(case when platform = 'Facebook' then new_followers else 0 end) as total_new_followers_facebook,
        round(avg(case when platform = 'Facebook' then new_followers end), 2) as avg_daily_new_followers_facebook,
        sum(case when platform = 'Facebook' then engaged_users else 0 end) as total_engaged_users_facebook,
        round(avg(case when platform = 'Facebook' then engaged_users end), 2) as avg_daily_engaged_users_facebook,
        sum(case when platform = 'Facebook' then website_clicks else 0 end) as total_website_clicks_facebook,
        round(avg(case when platform = 'Facebook' then website_clicks end), 2) as avg_daily_website_clicks_facebook,
        max(case when platform = 'Facebook' then total_followers end) as current_total_followers_facebook,

        -- Instagram metrics (total + daily average)
        sum(case when platform = 'Instagram' then new_followers else 0 end) as total_new_followers_instagram,
        round(avg(case when platform = 'Instagram' then new_followers end), 2) as avg_daily_new_followers_instagram,
        sum(case when platform = 'Instagram' then engaged_users else 0 end) as total_engaged_users_instagram,
        round(avg(case when platform = 'Instagram' then engaged_users end), 2) as avg_daily_engaged_users_instagram,
        sum(case when platform = 'Instagram' then website_clicks else 0 end) as total_website_clicks_instagram,
        round(avg(case when platform = 'Instagram' then website_clicks end), 2) as avg_daily_website_clicks_instagram,
        max(case when platform = 'Instagram' then total_followers end) as current_total_followers_instagram
    from social_with_client
    group by client_id
),

final as (
    select distinct
        cp.client_id,
        cp.client_name,
        cp.primary_industry,
        cp.platform,
        cp.attribution_window,

        -- Activity window & campaign footprint
        cp.first_activity_date,
        cp.last_activity_date,
        cp.num_campaigns,

        -- Paid media performance (client blended view)
        cp.total_impressions,
        cp.total_clicks,
        cp.total_conversions,
        cp.total_ad_spend_eur,
        cp.total_revenue_eur,
        cp.blended_ctr,
        cp.blended_cvr,
        cp.blended_cpc_eur,
        cp.blended_cpa_eur,
        cp.blended_roas,

        -- Media-only profitability (aggregated from OL model)
        cp.media_only_total_cost_eur_client      as media_only_total_cost_eur,
        cp.media_only_profit_eur_client          as media_only_profit_eur,
        cp.media_only_margin_on_revenue_client   as media_only_margin_on_revenue,
        cp.media_only_margin_on_cost_client      as media_only_margin_on_cost,

        -- Internal resource usage & cost
        ci.total_hours_all_projects,
        ci.total_internal_cost_eur,
        ci.productive_hours,
        ci.productive_internal_cost_eur,

        -- Net profit after internal cost (media-only profit minus internal cost)
        case
            when cp.media_only_profit_eur_client is not null and ci.total_internal_cost_eur is not null
            then round(cp.media_only_profit_eur_client - ci.total_internal_cost_eur, 2)
            else null
        end as net_profit_after_internal_eur,

        -- Profitability KPIs (Project Margin, Client LTV over observed period)
        case
            when cp.media_only_profit_eur_client is not null and ci.total_internal_cost_eur is not null
            and cp.total_revenue_eur > 0
            then round((cp.media_only_profit_eur_client - ci.total_internal_cost_eur) / cp.total_revenue_eur, 2)
            else null
        end as project_margin_after_internal,

        -- Approximate Client LTV over the observed period:
        -- net profit after internal cost accumulated across all campaigns in the dataset.
        case
            when cp.media_only_profit_eur_client is not null and ci.total_internal_cost_eur is not null
            then round(cp.media_only_profit_eur_client - ci.total_internal_cost_eur, 2)
            else null
        end as client_lifetime_value_eur,

        -- Social / organic performance
        cs.n_report_days_social,
        -- (Facebook)
        cs.n_report_days_facebook,
        cs.total_new_followers_facebook,
        cs.avg_daily_new_followers_facebook,
        cs.total_engaged_users_facebook,
        cs.avg_daily_engaged_users_facebook,
        cs.total_website_clicks_facebook,
        cs.avg_daily_website_clicks_facebook,
        cs.current_total_followers_facebook,
        -- (Instagram)
        cs.n_report_days_instagram,
        cs.total_new_followers_instagram,
        cs.avg_daily_new_followers_instagram,
        cs.total_engaged_users_instagram,
        cs.avg_daily_engaged_users_instagram,
        cs.total_website_clicks_instagram,
        cs.avg_daily_website_clicks_instagram,
        cs.current_total_followers_instagram

    from campaign_perf cp
    left join client_internal ci on cp.client_id = ci.client_id
    left join client_social cs on cp.client_id = cs.client_id
)

select distinct * from final

-- NOTE: This model is aggregated at client × platform × attribution_window.
--       Internal time and social metrics are only available at client (or client × channel) level,
--       so their values are repeated across platforms/windows and must be interpreted with care.
