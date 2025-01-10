-- sp_check_duplicate_records__colint_demo__etl.sql
-- SQL Object conversion metadata
-- conversion_id: 3e0e04da09dce8eb7b56351a531fe454
-- group_id: c1ba0a6be3057b6a97317f6aa7d44066
-- dbt_spoc_converter_sql_model_version: 1.2.0
-- model_conversion_started_at: 2024-12-20T17:11:53.581932+00:00
-- Source Statement SQL metadata
-- source_id: f281870581380c1fa9078140d9834e78

{{
    config(
        materialized='view',  -- Adjust materialization as needed
        database='colint_demo',
        schema='etl',
        meta={
            'input_sql_dialect': 'Snowflake SQL',
            'input_stored_procedure_runner': 'Azure Data Factory',
            'output_sql_dialect': 'Snowflake',
            'source_file_name': 'R__sp\\_*load*\\_d*policy*hist.sql',
            'description': 'Check AK constraint for duplicates in a temporary history table'
        }
    )
}}

WITH duplicate_check AS (
    SELECT
        policy_id,
        env_source_code,
        data_source_code,
        scd_effective_datetime,
        COUNT(*) AS record_count
    FROM {{ var('ctl_var__temp_hist_table') }}
    GROUP BY
        policy_id,
        env_source_code,
        data_source_code,
        scd_effective_datetime
    HAVING COUNT(*) > 1
)

SELECT 
    *
FROM duplicate_check;

/* 
Conversion Logic:
1. The stored procedure call has been converted into a CTE that checks for duplicate records based on the specified columns.
2. The use of variables from Azure Data Factory has been replaced with dbt variables using the `var` function.
3. The `COUNT(*) > 1` condition is used to identify duplicate records.
4. The materialization is set to 'view' for demonstration purposes; it should be adjusted based on the desired use case.
5. All metadata from the JSON input is included in the `meta` dictionary of the config block.

```

### Explanation of Conversion:

1. **Stored Procedure Call Conversion**: The original call to `etl.sp_check_duplicate_records` is transformed into a `WITH` clause (CTE) that performs a duplicate check by grouping on the specified columns and filtering for counts greater than one.

2. **Variable Replacement**: The `:temp_hist_table` variable from Azure Data Factory is replaced with a dbt variable using `{{ var('ctl_var__temp_hist_table') }}`. This assumes that the variable is correctly defined in the dbt environment.

3. **Materialization and Metadata**: The model is configured as a `view`, but this can be changed based on requirements. The metadata from the input JSON is added to the `meta` dictionary in the config block, ensuring traceability and context.

4. **Consistency and Comments**: The SQL code maintains the format and comments from the original code, ensuring readability and consistency with existing practices.

This transformation ensures the original logic is preserved while adopting dbt's conventions and capabilities.