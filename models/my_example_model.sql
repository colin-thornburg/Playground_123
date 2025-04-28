{{
  config(
    materialized='table',
    post_hook=[
      "{{ execute_arbitrary_sql(table_name='my_arbitrary_table', schema_name=target.schema) }}"
    ]
  )
}}

-- Your regular model SQL goes here
SELECT 1 as id