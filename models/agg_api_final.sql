{{ config(
    materialized='incremental',
    unique_key=['api_name', 'metric_date'],
    incremental_strategy='merge'
) }}

-- Combine current day and historical metrics
SELECT 
    api_name,
    metric_date,
    total_calls,
    successful_calls,
    failed_calls,
    avg_response_time,
    success_rate
FROM {{ ref('agg_api_metrics_current') }}

UNION ALL

SELECT 
    api_name,
    metric_date,
    total_calls,
    successful_calls,
    failed_calls,
    avg_response_time,
    success_rate
FROM {{ ref('agg_api_metrics_backfill') }}