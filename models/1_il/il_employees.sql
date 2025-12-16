{{ config(materialized='view') }}

-- Integration Layer view for employees
-- Grain: 1 row per employee_id

select
    *
from {{ source('raw', 'employees') }}
