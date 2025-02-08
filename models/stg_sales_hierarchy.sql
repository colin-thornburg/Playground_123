SELECT
    hierarchy_id,
    sfdc_user_id,
    reporting_segment,
    region_name,
    area_name,
    start_date,
    end_date
FROM {{ ref('raw_sales_hierarchy') }}