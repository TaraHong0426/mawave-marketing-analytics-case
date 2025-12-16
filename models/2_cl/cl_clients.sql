{{ config(materialized='table') }}

-- Clean Layer: clients
-- Grain: 1 row per client_id

-- Cleaning decisions based on DATA_EXPLORATION_GUIDE:
--  - Treat client_id as STRING (business keys should not rely on numeric type)
--  - Remove emojis from PRIMARY industry labels
--  - Normalize whitespace & casing for consistent matching
--  - SECONDARY industry is intentionally NOT normalized (kept as free-text tag)
--    → only invalid / technical artifacts are cleaned (e.g., Excel VLOOKUP errors)
--  - Convert empty strings to NULL 
--  - Standardize country formatting (title case, trimmed)

with cleaned as (
    select
        cast(client_id as varchar) as client_id, --PK (no FK: top-level dimension)
        trim(client_name) as client_name,
        trim(regexp_replace(primary_industry, '[^\x00-\x7F]', '')) as primary_industry_raw,
        nullif(trim(secondary_industry), '') as secondary_industry_raw,
        trim(country) as country
    from {{ ref('il_clients') }}
),

industries_cleaned as (
    select
        client_id,
        client_name,
        case
            when lower(primary_industry_raw) like '%finance%' then 'Finance, Insurance & Institutions'
            when lower(primary_industry_raw) like '%fashion%' then 'Fashion & Lifestyle'
            else primary_industry_raw
        end as primary_industry,
        case 
            when lower(secondary_industry_raw) like '#n/a%' then null
            when lower(secondary_industry_raw) like 'gern am mammaly%' then null
            else secondary_industry_raw
        end as secondary_industry, -- not reliable column
        country
    from cleaned
)

select *
from industries_cleaned
