with source as (
    select * from {{ ref('raw_sales_hierarchy') }}
),

staged as (
    select
        hierarchy_id,
        employee_id,
        parent_hierarchy_id,
        hierarchy_level,
        department_name,
        region_name,
        cast(effective_date as date) as effective_date,
        cast(end_date as date) as end_date,
        is_manager,
        team_size,
        current_timestamp() as dbt_insert_timestamp_utc
    from source
)

select * from staged