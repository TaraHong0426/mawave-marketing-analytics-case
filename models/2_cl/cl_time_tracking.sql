{{ config(materialized='table') }}

-- Clean Layer: time_tracking
-- Grain: 1 row per employee × client × (project opt) × report_date × activity

-- no single PK - fact table

with cleaned as (
    select
        cast(employee_id as varchar) as employee_id, --FK - cl_employees.employee_id
        cast(client_id as varchar) as client_id, --FK - cl_clients.client_id
        -- project_id = NULL + string
        case 
            when project_id is null then null
            else cast(cast(project_id as int) as varchar)
        end as project_id, --FK - cl_projects.project_id (nullable)
        trim(employee_name) as employee_name,
        trim(client_name) as client_name,
        cast(report_date as date) as report_date,
        cast(hours_worked as double) as hours_worked,
        trim(department_name) as department_name,
        cast(cost_eur as double) as cost_eur,
        is_productive
    from {{ ref('il_time_tracking') }}
)

select *
from cleaned
