{{ config(materialized='table') }}

-- Clean Layer: employees
-- Grain: 1 row per employee_id

with cleaned as (
    select
        cast(employee_id as varchar) as employee_id, --PK
        trim(employee_name) as employee_name,
        nullif(trim(department_name), '') as department_name,
        nullif(trim(team_name), '') as team_name,
        cast(hourly_rate_eur as double) as hourly_rate_eur,
        upper(trim(status)) as status
    from {{ ref('il_employees') }}
)

select *
from cleaned