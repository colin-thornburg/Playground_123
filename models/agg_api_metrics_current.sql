/*
This model processes the current day's data (2024-01-10 in our scenario)
*/

{{ config(
    materialized='incremental',
    unique_key=['api_name', 'metric_date'],
    incremental_strategy='merge'
) }}

{% set current_date = "2024-01-10" %}

WITH current_metrics AS (
    SELECT 
        api_name,
        DATE(event_time) as metric_date,
        COUNT(*) as total_calls,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_calls,
        SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_calls,
        AVG(value) as avg_response_time
    FROM {{ ref('stage_api_metrics') }}
    WHERE DATE(event_time) = '{{ current_date }}'
    GROUP BY 1, 2
)

SELECT 
    api_name,
    metric_date,
    total_calls,
    successful_calls,
    failed_calls,
    avg_response_time,
    (successful_calls::float / NULLIF(total_calls, 0)) * 100 as success_rate
FROM current_metrics