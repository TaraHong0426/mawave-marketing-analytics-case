{{ config(materialized='view') }}

-- Integration Layer view for projects
-- Grain: 1 row per project_id

select
    *
from {{ source('raw', 'projects') }}
