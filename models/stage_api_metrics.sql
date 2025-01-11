/*
Scenario Part 1 & 3: "15 new records came into the source table" & "loads all records into a stage table"
This staging model captures ALL incoming records regardless of their event_time:
- New current day records (5 from 01/10/2025)
- Recent late-arriving records (5 from 01/09/2025)
- Historical late-arriving records (5 from 01/05/2025)
*/


{{ config(
    materialized='incremental',
    unique_key='metric_id',
    incremental_strategy='merge'
) }}

-- Stage model captures ALL new records regardless of event_time
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
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}