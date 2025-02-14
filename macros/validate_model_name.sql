{% macro validate_model_name(model_to_validate) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set path = model_to_validate.path %}
    {% set name = model_to_validate.name %}

    {# Validate staging models #}
    {% if '/staging/' in path and not name.startswith('stg_') %}
        {{ exceptions.raise_compiler_error("Invalid model name. Staging models must start with 'stg_'. Got: " ~ name) }}
    {% endif %}

    {# Validate target models #}
    {% if '/target/' in path and not name.startswith('tgt_') %}
        {{ exceptions.raise_compiler_error("Invalid model name. Target models must start with 'tgt_'. Got: " ~ name) }}
    {% endif %}

    {# Validate published models #}
    {% if '/published/' in path and not name.startswith('pub_') %}
        {{ exceptions.raise_compiler_error("Invalid model name. Published models must start with 'pub_'. Got: " ~ name) }}
    {% endif %}
    
    {# Optional: Validate corresponding target model exists for published models #}
    {% if '/published/' in path %}
        {% set target_model_name = 'tgt_' ~ name[4:] %}  {# Strip 'pub_' and add 'tgt_' #}
        {% if target_model_name not in graph.nodes %}
            {{ exceptions.raise_compiler_error("Published model '" ~ name ~ "' must have a corresponding target model named '" ~ target_model_name ~ "'") }}
        {% endif %}
    {% endif %}
{% endmacro %}