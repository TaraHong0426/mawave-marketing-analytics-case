{{ config(materialized='view') }}

-- Integration Layer view for time tracking logs
-- Grain: 1 row per employee_id, client_id, report_date

select
    *
from {{ source('raw', 'time_tracking') }}
