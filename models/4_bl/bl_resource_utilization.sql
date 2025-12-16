{{ config(materialized='table') }}

-- Business Layer: Resource Utilization
-- Grain: 1 row per employee_id × client_id × project_id
-- Sources:
--   - cl_time_tracking 
--   - cl_employees (name, department, team, hourly_rate_eur and status)
--   - cl_clients (client_name, industry and country)
--   - cl_projects (project_name, start/end dates and budget information)
-- Business Purpose:
--   Employee utilization (billable vs non-billable) by client/project.
--   Supports resource planning, cost analysis, and project profitability insights.

-- NOTE:
--   In the provided sample dataset, ALL time entries have is_productive = TRUE.
--   As a result:
--       utilization_rate_billable_hours = 1
--       billable_hours = total_hours
--       non_billable_hours = 0 
--   The logic below is kept GENERIC to reflect real situation where both billable and non-billable time exist. 
--   This makes the model future-proof and aligned with industry standards.

with main as (
    select
        tt.employee_id,
        e.employee_name,
        e.department_name,
        e.team_name,
        e.hourly_rate_eur,
        e.status as employee_status,
        tt.client_id,
        c.client_name,
        c.primary_industry,
        c.country,
        tt.project_id,
        p.project_name,
        p.project_start_date,
        p.project_end_date,
        p.monthly_budget_eur,
        tt.report_date,
        tt.hours_worked,
        tt.cost_eur,
        tt.is_productive
    from {{ ref('cl_time_tracking') }} tt
    left join {{ ref('cl_employees') }} e on tt.employee_id = e.employee_id
    left join {{ ref('cl_clients') }} c on tt.client_id = c.client_id
    left join {{ ref('cl_projects') }} p on tt.project_id = p.project_id
),

agg as (
    select
        employee_id,
        employee_name,
        department_name,
        team_name,
        hourly_rate_eur,
        employee_status,
        client_id,
        client_name,
        primary_industry,
        country,
        project_id,
        project_name,
        project_start_date,
        project_end_date,
        monthly_budget_eur,
        min(report_date) as first_work_date,
        max(report_date) as last_work_date,
        count(distinct report_date) as n_work_days,
        sum(hours_worked) as total_hours,
        -- generic logic
        sum(case when is_productive then hours_worked else 0 end) as billable_hours,
        sum(case when not is_productive then hours_worked else 0 end) as non_billable_hours,
        sum(cost_eur) as total_cost_eur,
        sum(case when is_productive then cost_eur else 0 end) as billable_cost_eur,
        sum(case when not is_productive then cost_eur else 0 end) as non_billable_cost_eur
    from main
    group by
        employee_id,
        employee_name,
        department_name,
        team_name,
        hourly_rate_eur,
        employee_status,
        client_id,
        client_name,
        primary_industry,
        country,
        project_id,
        project_name,
        project_start_date,
        project_end_date,
        monthly_budget_eur
),

final as (
    select
        -- dimension columns
        employee_id,
        employee_name,
        department_name,
        team_name,
        hourly_rate_eur,
        employee_status,
        client_id,
        client_name,
        primary_industry,
        country,
        project_id,
        project_name,
        project_start_date,
        project_end_date,
        monthly_budget_eur,

        -- aggregated date & volume columns
        first_work_date,
        last_work_date,
        n_work_days,

        -- round raw aggregates to keep numbers readable
        round(total_hours, 2) as total_hours,
        round(billable_hours, 2) as billable_hours,
        round(non_billable_hours, 2) as non_billable_hours,
        round(total_cost_eur, 2) as total_cost_eur,
        round(billable_cost_eur, 2) as billable_cost_eur,
        round(non_billable_cost_eur, 2) as non_billable_cost_eur,

        -- Utilization based on hours
        case
            when total_hours > 0 then round(billable_hours * 1.0 / total_hours, 4)
            else null
        end as utilization_rate_billable_hours,

        case
            when total_cost_eur > 0 then round(billable_cost_eur * 1.0 / total_cost_eur, 4)
            else null
        end as utilization_rate_billable_cost,
        
        -- Workload KPI: average hours per working day
        case
            when n_work_days > 0 then round(total_hours * 1.0 / n_work_days, 2)
            else null
        end as avg_hours_per_work_day,

        -- Empirical utilization: total tracked hours / (n_work_days × 8h)-- altaneative billable
        -- This approximates how fully the employee logged their expected working day.
        case
            when n_work_days > 0 then round(total_hours * 1.0 / (n_work_days * 8), 4)
            else null
        end as empirical_utilization_rate,

        -- Cost KPI: effective internal cost per tracked hour
        case
            when total_hours > 0 then round(total_cost_eur * 1.0 / total_hours, 2)
            else null
        end as internal_cost_per_hour_eur
    from agg
)

select * from final
