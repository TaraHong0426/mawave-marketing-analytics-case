{{ config(materialized='table') }}

-- Clean Layer: projects
-- Grain: 1 row per project_id

with cleaned as (

    select
        cast(project_id as varchar) as project_id, --PK
        cast(client_id as varchar) as client_id, --FK - cl_clients.client_id
        trim(client_name) as client_name,
        trim(project_name) as project_name, --## only Main Campaign
        cast(project_start_date as date) as project_start_date, --## 2024-01-01
        cast(project_end_date as date) as project_end_date, --## 2024-12-31
        cast(monthly_budget_eur as double) as monthly_budget_eur
    from {{ ref('il_projects') }}
)

select *
from cleaned
