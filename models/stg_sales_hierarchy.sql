SELECT
    hierarchy_id,
    sfdc_user_id,
    reporting_segment,
    start_date,
    end_date
FROM {{ ref('raw_sales_hierarchy') }}