{% macro test_coverage(model_folder) %}
    {% if execute %}
        {% set models_without_tests = [] %}
        
        {% for node_name, node in graph.nodes.items() %}
            {% if node.resource_type == 'model' and node.path.startswith(model_folder) %}
                {# Get all tests that reference this model #}
                {% set test_count = 0 %}
                {% for test_name, test in graph.nodes.items() %}
                    {% if test.resource_type == 'test' and node.unique_id in test.depends_on.nodes %}
                        {% set test_count = test_count + 1 %}
                    {% endif %}
                {% endfor %}
                
                {# If no tests found, add to our list #}
                {% if test_count == 0 %}
                    {% do models_without_tests.append(node.name) %}
                {% endif %}
            {% endif %}
        {% endfor %}
        
        {% if models_without_tests|length > 0 %}
            {{ log("The following models in '" ~ model_folder ~ "' have no tests: " ~ models_without_tests|join(', '), info=True) }}
            {{ exceptions.raise_compiler_error("Test coverage validation failed. " ~ models_without_tests|length ~ " models lack tests.") }}
        {% else %}
            {{ log("All models in '" ~ model_folder ~ "' have at least one test! ✅", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}