{{
    config(
        materialized='incremental'
    )
}}

Select 1 from {{ ref('my_example_model') }}