WITH base AS (
    SELECT 
        o.opportunity_id,
        o.created_date,
        o.owner_id,
        o.created_date_user_id_concat,
        sh.hierarchy_id,
        sh.reporting_segment
    FROM {{ ref('stg_opportunities') }} o
    LEFT JOIN {{ ref('stg_sales_hierarchy') }} sh
        ON o.owner_id = sh.sfdc_user_id
        AND o.created_date >= sh.start_date 
        AND o.created_date < sh.end_date
)
SELECT * FROM base