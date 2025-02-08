SELECT
    cx_walk_key,
    user_id,
    date,
    hierarchy_id
FROM {{ ref('sh_sl_crosswalk_seed') }}
