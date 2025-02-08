with opportunities as (
    select * from {{ ref('stg_sf_opportunity') }}
),

final as (
    select
        -- Primary key
        opportunity_id,
        
        -- Foreign keys
        owner_id,
        account_id,
        
        -- Timestamps
        created_date,
        close_date,
        
        -- Measures
        amount,
        
        -- Dimensions
        stage_name,
        is_won,
        is_closed,
        
        -- Time-based hierarchy join keys
        created_date_user_id_concat,
        close_date_user_id_concat,
        lost_date_user_id_concat,
        submitted_date_user_id_concat
    from opportunities
)

select * from final
