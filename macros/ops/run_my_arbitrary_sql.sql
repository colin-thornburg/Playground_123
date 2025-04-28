{% macro run_my_arbitrary_sql(table_name, schema_name) %}
  {{ log("Starting arbitrary SQL operation...", info=True) }}

  {# Explicitly render the schema_name argument in case it contains Jinja #}
  {% set rendered_schema_name = render(schema_name) %}
  {{ log("Raw schema_name arg: " ~ schema_name, info=True) }}
  {{ log("Rendered schema name: " ~ rendered_schema_name, info=True) }}


  {# Pass the rendered schema name to the execution macro #}
  {% do execute_arbitrary_sql(table_name, rendered_schema_name) %}

  {{ log("Finished arbitrary SQL operation.", info=True) }}
{% endmacro %}