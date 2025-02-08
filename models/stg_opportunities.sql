SELECT
    opportunity_id,
    owner_id,
    amount,
    created_date,
    submitted_date,
    -- Create concat keys for joins
    CONCAT(created_date, '_', owner_id) as created_date_user_id_concat,
    CONCAT(submitted_date, '_', owner_id) as submitted_date_user_id_concat
FROM {{ ref('raw_opportunities') }}