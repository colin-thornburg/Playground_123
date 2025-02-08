{{ 
    config(
        materialized='table'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}
),

hierarchy_dates as (
    select distinct
        hierarchy_id,
        effective_date,
        end_date
    from {{ ref('stg_sf_sales_hierarchy') }}
),

bridge as (
    select
        h.hierarchy_id,
        d.date_day as date_id,
        concat(h.hierarchy_id, '_', d.date_day) as hierarchy_date_key
    from hierarchy_dates h
    cross join date_spine d
    where d.date_day >= h.effective_date
    and d.date_day <= h.end_date
)

select * from bridge