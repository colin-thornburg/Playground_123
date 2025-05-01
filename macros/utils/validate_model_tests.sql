{% macro validate_model_tests(model_folder) %}
    {% set models_without_tests = [] %}
    
    {% for model in graph.nodes.values() %}
        {# Check if it's a model in the specified folder #}
        {% if model.resource_type == 'model' and model.path.startswith(model_folder) %}
            {# Get all tests that reference this model #}
            {% set model_tests = [] %}
            
            {% for test in graph.nodes.values() %}
                {% if test.resource_type == 'test' and model.unique_id in test.depends_on.nodes %}
                    {% do model_tests.append(test.unique_id) %}
                {% endif %}
            {% endfor %}
            
            {# If no tests found, add to our list #}
            {% if model_tests|length == 0 %}
                {% do models_without_tests.append(model.name) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    
    {# Raise error if any models lack tests #}
    {% if models_without_tests|length > 0 %}
        {{ exceptions.raise_compiler_error("The following models in '" ~ model_folder ~ "' have no tests: " ~ models_without_tests|join(', ')) }}
    {% endif %}
{% endmacro %}