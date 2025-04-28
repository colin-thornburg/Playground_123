{% macro execute_arbitrary_sql(table_name, schema_name) %}
  {% set create_sql %}
  CREATE TABLE IF NOT EXISTS {{ schema_name }}.{{ table_name }} (
      id INT,
      description VARCHAR
  );
  {% endset %}

  {% set insert_sql %}
  INSERT INTO {{ schema_name }}.{{ table_name }} (id, description) VALUES
  (1, 'First row'),
  (2, 'Second row');
  {% endset %}

  {{ log("Executing create table statement: " ~ create_sql, info=True) }}
  {% do run_query(create_sql) %}

  {{ log("Executing insert statement: " ~ insert_sql, info=True) }}
  {% do run_query(insert_sql) %}

  {{ log("Finished executing arbitrary SQL for table " ~ schema_name ~ "." ~ table_name, info=True) }}
{% endmacro %}