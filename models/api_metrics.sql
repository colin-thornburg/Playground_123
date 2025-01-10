{{ config(
    materialized='incremental',
    unique_key='metric_id',
    incremental_strategy='merge'
) }}

SELECT 
    metric_id,
    api_name,
    event_time,
    value,
    status,
    inserted_at,
    updated_at
FROM {{ ref('api_metrics_seed') }}
{% if is_incremental() %}
WHERE event_time > DATEADD(day, -1, (SELECT MAX(event_time) FROM {{ this }}))
    AND updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}