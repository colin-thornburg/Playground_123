-- udf_get_env_variables__colint_demo__dbt_cthornburg.sql
-- SQL Object conversion metadata
-- conversion_id: 3e0e04da09dce8eb7b56351a531fe454
-- group_id: ab663a86abd5425e43cb99c22c5f5928
-- dbt_spoc_converter_sql_model_version: 1.2.0
-- model_conversion_started_at: 2024-12-20T17:07:20.728414+00:00
-- Source Statement SQL metadata
-- conversion_id: 3e0e04da09dce8eb7b56351a531fe454
-- source_id: d27757ca572b96cb99cd1f7dcbf3ca67

{{
    config(
        materialized='table',
        database='colint_demo',
        schema='dbt_cthornburg',
        alias='udf_get_env_variables',
        meta={
            'input_sql_dialect': 'Snowflake SQL',
            'input_stored_procedure_runner': 'Azure Data Factory',
            'output_sql_dialect': 'Snowflake',
            'source_file_name': 'R__sp\\_*load*\\_d*policy*hist.sql'
        }
    )
}}

WITH env_variables AS (
    -- Get environment variables
    SELECT * FROM {{ ref('udf_get_env_variables__colint_demo__etl') }}
)

SELECT * FROM env_variables;

/* 
Conversion Logic:
1. The stored procedure logic was converted to a dbt model using a CTE to simulate the retrieval of environment variables.
2. The reference to the `udf_get_env_variables` function is replaced with a reference to a dbt model using the `ref` macro.
3. The config block includes metadata information as specified in the input JSON.
4. SQL comments and metadata information are preserved and included in the final model.
5. The SQL dialects and stored procedure runner metadata are included in the config block's meta dictionary.
*/