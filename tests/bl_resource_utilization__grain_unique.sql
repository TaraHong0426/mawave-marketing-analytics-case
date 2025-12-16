-- tests/bl_resource_utilization__grain_unique.sql

with base as (
    select
        employee_id,
        client_id,
        project_id,
        count(*) as row_count
    from {{ ref('bl_resource_utilization') }}
    group by
        employee_id, client_id, project_id
)

select * from base
where row_count > 1
