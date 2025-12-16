{{ config(materialized='view') }}

-- Integration Layer view for clients
-- Grain: 1 row per client_id

select
    *
from {{ source('raw', 'clients') }}
