{% macro model_naming_standards_check() %}
    {{ log('Validating object: ' ~ this.identifier, info=True) }}
    
    {# Staging models validation #}
    {% if 'staging' in model.path %}
        {% if not model.name.startswith('stg_') %}
            {{ exceptions.raise_compiler_error('Invalid naming convention. Staging object must start with "stg_": ' ~ model.path) }}
        {% endif %}
    {% endif %}
    
    {# Target models validation #}
    {% if 'target' in model.path %}
        {% if not model.name.startswith('tgt_') %}
            {{ exceptions.raise_compiler_error('Invalid naming convention. Target object must start with "tgt_": ' ~ model.path) }}
        {% endif %}
    {% endif %}
    
    {# Published models validation #}
    {% if 'published' in model.path %}
        {% if not model.name.startswith('pub_') %}
            {{ exceptions.raise_compiler_error('Invalid naming convention. Published object must start with "pub_": ' ~ model.path) }}
        {% endif %}
        
        {# Validate corresponding target model exists #}
        {% set target_model_name = 'tgt_' ~ model.name[4:] %}
        {% if target_model_name not in graph.nodes %}
            {{ exceptions.raise_compiler_error('Published model must have corresponding target model. Expected: ' ~ target_model_name ~ ' for published model: ' ~ model.path) }}
        {% endif %}
    {% endif %}
    
{% endmacro %}