with hierarchy as (
    select * from {{ ref('stg_sf_sales_hierarchy') }}
),

final as (
    select
        hierarchy_id,
        employee_id,
        parent_hierarchy_id,
        hierarchy_level,
        department_name,
        region_name,
        effective_date,
        end_date,
        is_manager,
        team_size,
        dbt_insert_timestamp_utc
    from hierarchy
)

select * from final