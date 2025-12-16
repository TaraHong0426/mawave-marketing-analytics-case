{{ config(materialized='table') }}

-- Operational Layer: Client Projects With Time
-- Grain: 1 row per client_id × project_id × report_date

-- Keys:
--   - client_id (FK → cl_clients.client_id)
--   - project_id (FK → cl_projects.project_id, nullable in raw but filtered here)

-- Sources:
--   - cl_time_tracking
--   - cl_projects
--   - cl_clients
--   - cl_employees (for distinct_employees)

with main as (
    select
        t.client_id,
        cl.client_name,
        cl.primary_industry,
        cl.country,
        t.project_id,
        p.project_name,
        p.project_start_date,
        p.project_end_date,
        p.monthly_budget_eur,
        t.report_date,
        t.employee_id,
        e.employee_name,
        e.department_name as employee_department,
        t.hours_worked,
        t.cost_eur,
        t.is_productive
    from {{ ref('cl_time_tracking') }} as t
    -- client 
    left join {{ ref('cl_clients') }} as cl on t.client_id = cl.client_id
    -- project
    left join {{ ref('cl_projects') }} as p on t.project_id = p.project_id
    -- employee (distinct_employees calc)
    left join {{ ref('cl_employees') }} as e on t.employee_id = e.employee_id
),

project_agg as (
    select
        client_id,
        client_name,
        primary_industry,
        country,
        project_id,
        project_name,
        project_start_date,
        project_end_date,
        monthly_budget_eur,
        report_date,
        count(distinct employee_id) as n_employees,
        sum(hours_worked) as total_hours,
        sum(cost_eur) as total_cost_eur,

        -- productive / non-productive -- no need here but for extension design
        sum(case when is_productive then hours_worked else 0 end) as productive_hours,
        sum(case when not is_productive then hours_worked else 0 end) as non_productive_hours,
        sum(case when is_productive then cost_eur else 0 end) as productive_cost_eur,
        sum(case when not is_productive then cost_eur else 0 end) as non_productive_cost_eur
    from main
    group by
        client_id,
        client_name,
        primary_industry,
        country,
        project_id,
        project_name,
        project_start_date,
        project_end_date,
        monthly_budget_eur,
        report_date
)

select *
from project_agg
