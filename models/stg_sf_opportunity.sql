with source as (
    select * from {{ ref('raw_sf_opportunity') }}
),

staged as (
    select
        -- Primary key
        opportunity_id,
        
        -- Foreign keys
        owner_id,
        account_id,
        
        -- Measures
        amount,
        
        -- Dimensions
        stage_name,
        try_to_date(created_date) as created_date,
        try_to_date(close_date) as close_date,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        
        -- Derived fields for the semantic layer
        owner_id as user_id,  -- Alias for joining to hierarchy
        
        -- Fields for time-based hierarchy joins
        concat(created_date, '_', owner_id) as created_date_user_id_concat,
        concat(close_date, '_', owner_id) as close_date_user_id_concat,
        case 
            when stage_name = 'Lost' 
            then concat(close_date, '_', owner_id) 
        end as lost_date_user_id_concat,
        case 
            when stage_name = 'Submitted' 
            then concat(created_date, '_', owner_id) 
        end as submitted_date_user_id_concat
        
    from source
),

final as (
    select 
        *,
        case
            when stage_name = 'Closed Won' then true
            when stage_name = 'Lost' then false
            else null
        end as is_won,
        
        case
            when stage_name in ('Closed Won', 'Lost') then true
            else false
        end as is_closed
    from staged
)

select * from final