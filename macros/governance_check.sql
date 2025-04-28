{% macro governance_check() %}
    {{ log('Governance check – validating: ' ~ this.identifier, info=True) }}

    {# ---------- 1. Naming convention for staging models ---------- #}
    {% if 'staging' in model.path %}
        {% if not model.name.startswith('stg_') %}
            {{ exceptions.raise_compiler_error(
                'Invalid naming convention: staging models must start with "stg_". Offender → ' ~ model.path
            ) }}
        {% endif %}
    {% endif %}

    {# ---------- 2. Incremental-strategy enforcement for transactional models ---------- #}
    {% if 'transactional' in model.path %}
        {% set mat_type = model.config.materialized or 'view' %}
        {% if mat_type != 'incremental' %}
            {{ exceptions.raise_compiler_error(
                'Transactional models must use incremental materialization. '
                ~ 'Found "' ~ mat_type ~ '" in ' ~ model.path
            ) }}
        {% endif %}
    {% endif %}

{% endmacro %}

