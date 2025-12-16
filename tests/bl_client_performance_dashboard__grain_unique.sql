-- tests/bl_client_performance_dashboard__grain_unique.sql

with base as (
    select
        client_id,
        platform,
        attribution_window,
        count(*) as row_count
    from {{ ref('bl_client_performance_dashboard') }}
    group by
        client_id, platform, attribution_window
)

select * from base
where row_count > 1
