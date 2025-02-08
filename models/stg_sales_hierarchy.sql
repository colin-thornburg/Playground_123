SELECT
    hierarchy_id,
    region_name,
    start_date,
    end_date
FROM {{ ref('sales_hierarchy_seed') }}
