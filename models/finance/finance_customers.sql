{{
    config(
        schema='my_finance_schema',
        alias='customers'
    )
}}

Select * from {{ ref('sales_hierarchy_seed') }}