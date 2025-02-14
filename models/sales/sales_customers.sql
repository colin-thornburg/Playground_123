{{
    config(
        schema='my_sales_schema',
        alias='customers'
    )
}}


Select * from {{ ref('sales_hierarchy_seed') }}