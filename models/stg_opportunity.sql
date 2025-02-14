SELECT
    opportunity_id,
    user_id,
    created_date,
    submitted_date,
    -- Create “composite” keys for each date role
    CAST(created_date AS STRING) AS created_date_user_id_concat,
    CAST(submitted_date AS STRING) AS submitted_date_user_id_concat,
    amount
FROM {{ ref('opportunity_seed') }}
