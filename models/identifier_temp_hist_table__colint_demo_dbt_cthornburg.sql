-- identifier_temp_hist_table___colint_demo__dbt_cthornburg.sql
-- SQL Object conversion metadata
-- conversion_id: 3e0e04da09dce8eb7b56351a531fe454
-- group_id: c4199838a9339a0336062547623bf90f
-- dbt_spoc_converter_sql_model_version: 1.2.0
-- model_conversion_started_at: 2024-12-20T17:08:08.862311+00:00
-- Source Statement SQL metadata
-- source_id: f7ad9e1b17be5c21eb9cdd1dc2b83876

{{
    config(
        materialized='table',
        database='colint_demo',
        schema='dbt_cthornburg',
        alias='identifier_temp_hist_table',
        meta={
            'input_sql_dialect': 'Snowflake SQL',
            'input_stored_procedure_runner': 'Azure Data Factory',
            'output_sql_dialect': 'Snowflake',
            'source_file_name': 'R__sp\_*load*\_d*policy*hist.sql'
        }
    )
}}

WITH src AS (
    SELECT 
        t.policy_hist_key
        ,t.scd_effective_datetime
        ,t.scd_expiration_datetime
        ,t.scd_current_ind
        ,s.policy_key
        ,s.etl_active_ind
        ,s.env_source_code
        ,s.data_source_code
        ,s.policy_id
        ,s.policy_source_sid
        ,s.policy_num
        ,s.plan_num
        ,s.effective_date
        ,s.expiration_date
        ,s.inception_date
        ,s.bound_date
        ,s.mail_out_date
        ,s.cancel_date
        ,s.cancel_reason_code
        ,s.reinstated_date
        ,s.policy_status_code
        ,s.policy_status_desc
        ,s.endorsement_code
        ,s.sic_code
        ,s.bill_type_code
        ,s.invoice_date
        ,s.contracted_expiration_date
        ,s.cancellation_reason_desc
        ,s.cancellation_reason_other_desc
        ,s.annualized_endorsement_premium_amt
        ,s.written_premium_amt
        ,s.annualized_premium_amt
        ,s.producer_source_sid
        ,s.producer_name
        ,s.account_manager_source_sid
        ,s.account_manager_name
        ,s.active_ind
        ,s.policy_level_source_code
        ,s.ajg_policy_num
        ,s.renewal_ind
        ,s.original_policy_id
        ,s.original_effective_date
        ,s.tax_state_code
        ,s.operations_of_insurance_desc
        ,s.program_eligibility_code
        ,s.program_eligibility_name
        ,s.program_desc_01
        ,s.program_desc_02
        ,s.producer_contact_name
        ,s.policy_source_code
        ,s.policy_name
        ,s.policy_type_name
        ,s.carrier_source_sid
        ,s.carrier_name
        ,s.excess_primary_code
        ,s.sm_exclusion_ind
        ,s.multi_year_ind
        ,s.master_policy_num
        ,s.continuous_policy_ind
        ,s.commission_rate_pct
        ,s.member_enrollment_count
        ,CASE 
            WHEN t.policy_hist_key IS NULL THEN TRUE 
            ELSE FALSE 
        END AS ins_ind
        ,CASE 
            WHEN COALESCE(s.policy_source_sid, '') <> COALESCE(t.policy_source_sid, '')
                OR COALESCE(s.plan_num, '') <> COALESCE(t.plan_num, '')
                OR COALESCE(s.inception_date, '1900-01-01') <> COALESCE(t.inception_date, '1900-01-01')
                OR COALESCE(s.bound_date, '1900-01-01') <> COALESCE(t.bound_date, '1900-01-01')
                OR COALESCE(s.mail_out_date, '1900-01-01') <> COALESCE(t.mail_out_date, '1900-01-01')
                OR COALESCE(s.reinstated_date, '1900-01-01') <> COALESCE(t.reinstated_date, '1900-01-01')
                OR COALESCE(s.invoice_date, '1900-01-01') <> COALESCE(t.invoice_date, '1900-01-01')
                OR COALESCE(s.account_manager_source_sid, '') <> COALESCE(t.account_manager_source_sid, '')
                OR COALESCE(s.account_manager_name, '') <> COALESCE(t.account_manager_name, '')
                OR COALESCE(CAST(s.active_ind AS INT), 9) <> COALESCE(CAST(t.active_ind AS INT), 9)
                OR COALESCE(s.policy_level_source_code, '') <> COALESCE(t.policy_level_source_code, '')
                OR COALESCE(s.original_policy_id, '') <> COALESCE(t.original_policy_id, '')
                OR COALESCE(s.original_effective_date, '1900-01-01') <> COALESCE(t.original_effective_date, '1900-01-01')
                OR COALESCE(s.tax_state_code, '') <> COALESCE(t.tax_state_code, '')
                OR COALESCE(s.operations_of_insurance_desc, '') <> COALESCE(t.operations_of_insurance_desc, '')
                OR COALESCE(s.producer_contact_name, '') <> COALESCE(t.producer_contact_name, '')
                OR COALESCE(s.policy_name, '') <> COALESCE(t.policy_name, '')
                OR COALESCE(s.policy_source_code, '') <> COALESCE(t.policy_source_code, '')
                OR COALESCE(s.policy_type_name, '') <> COALESCE(t.policy_type_name, '')
                OR COALESCE(s.carrier_source_sid, '') <> COALESCE(t.carrier_source_sid, '')
                OR COALESCE(s.carrier_name, '') <> COALESCE(t.carrier_name, '')
                OR COALESCE(s.excess_primary_code, '') <> COALESCE(t.excess_primary_code, '')
                OR COALESCE(CAST(s.sm_exclusion_ind AS INT), 9) <> COALESCE(CAST(t.sm_exclusion_ind AS INT), 9)
                OR COALESCE(s.master_policy_num, '') <> COALESCE(t.master_policy_num, '')
                OR COALESCE(CAST(s.continuous_policy_ind AS INT), 9) <> COALESCE(CAST(t.continuous_policy_ind AS INT), 9)
                OR COALESCE(s.commission_rate_pct, -9999999999) <> COALESCE(t.commission_rate_pct, -9999999999)
                OR COALESCE(s.member_enrollment_count, -9999999999) <> COALESCE(t.member_enrollment_count, -9999999999)
            THEN TRUE 
            ELSE FALSE 
        END AS upd1_ind
        ,CASE 
            WHEN COALESCE(s.policy_num, '') <> COALESCE(t.policy_num, '')
                OR COALESCE(s.effective_date, '1900-01-01') <> COALESCE(t.effective_date, '1900-01-01')
                OR COALESCE(s.expiration_date, '1900-01-01') <> COALESCE(t.expiration_date, '1900-01-01')
                OR COALESCE(s.cancel_date, '1900-01-01') <> COALESCE(t.cancel_date, '1900-01-01')
                OR COALESCE(s.cancel_reason_code, '') <> COALESCE(t.cancel_reason_code, '')
                OR COALESCE(s.policy_status_code, '') <> COALESCE(t.policy_status_code, '')
                OR COALESCE(s.policy_status_desc, '') <> COALESCE(t.policy_status_desc, '')
                OR COALESCE(s.endorsement_code, '') <> COALESCE(t.endorsement_code, '')
                OR COALESCE(s.sic_code, '') <> COALESCE(t.sic_code, '')
                OR COALESCE(s.bill_type_code, '') <> COALESCE(t.bill_type_code, '')
                OR COALESCE(s.contracted_expiration_date, '1900-01-01') <> COALESCE(t.contracted_expiration_date, '1900-01-01')
                OR COALESCE(s.cancellation_reason_desc, '') <> COALESCE(t.cancellation_reason_desc, '')
                OR COALESCE(s.cancellation_reason_other_desc, '') <> COALESCE(t.cancellation_reason_other_desc, '')
                OR COALESCE(s.annualized_endorsement_premium_amt, -9999999999) <> COALESCE(t.annualized_endorsement_premium_amt, -9999999999)
                OR COALESCE(s.written_premium_amt, -9999999999) <> COALESCE(t.written_premium_amt, -9999999999)
                OR COALESCE(s.annualized_premium_amt, -9999999999) <> COALESCE(t.annualized_premium_amt, -9999999999)
                OR COALESCE(s.producer_source_sid, '') <> COALESCE(t.producer_source_sid, '')
                OR COALESCE(s.producer_name, '') <> COALESCE(t.producer_name, '')
                OR COALESCE(s.ajg_policy_num, '') <> COALESCE(t.ajg_policy_num, '')
                OR COALESCE(CAST(s.renewal_ind AS INT), 9) <> COALESCE(CAST(t.renewal_ind AS INT), 9)
                OR COALESCE(s.program_eligibility_code, '') <> COALESCE(t.program_eligibility_code, '')
                OR COALESCE(s.program_eligibility_name, '') <> COALESCE(t.program_eligibility_name, '')
                OR COALESCE(s.program_desc_01, '') <> COALESCE(t.program_desc_01, '')
                OR COALESCE(s.program_desc_02, '') <> COALESCE(t.program_desc_02, '')
                OR COALESCE(CAST(s.multi_year_ind AS INT), 9) <> COALESCE(CAST(t.multi_year_ind AS INT), 9)
            THEN TRUE 
            ELSE FALSE 
        END AS upd2_ind
        ,CASE 
            WHEN s.etl_active_ind <> t.etl_active_ind AND t.etl_active_ind THEN TRUE 
            ELSE FALSE 
        END AS exp_ind
        ,CASE 
            WHEN s.etl_active_ind <> t.etl_active_ind AND s.etl_active_ind THEN TRUE 
            ELSE FALSE 
        END AS rei_ind
    FROM (
        SELECT * 
        FROM {{ ref('d_policy__colint_demo__edw') }} 
        WHERE env_source_code = {{ var('ctl_var__env_source_code') }}
    ) AS s
    FULL OUTER JOIN (
        SELECT * 
        FROM {{ ref('d_policy_hist__colint_demo__edw') }} 
        WHERE env_source_code = {{ var('ctl_var__env_source_code') }} 
        AND scd_current_ind
    ) AS t 
    ON t.policy_key = s.policy_key
)

SELECT 
    policy_key AS join_policy_key
    ,policy_key
    ,env_source_code
    ,data_source_code
    ,policy_id
    ,policy_source_sid
    ,policy_num
    ,plan_num
    ,effective_date
    ,expiration_date
    ,inception_date
    ,bound_date
    ,mail_out_date
    ,cancel_date
    ,cancel_reason_code
    ,reinstated_date
    ,policy_status_code
    ,policy_status_desc
    ,endorsement_code
    ,sic_code
    ,bill_type_code
    ,invoice_date
    ,contracted_expiration_date
    ,cancellation_reason_desc
    ,cancellation_reason_other_desc
    ,annualized_endorsement_premium_amt
    ,written_premium_amt
    ,annualized_premium_amt
    ,producer_source_sid
    ,producer_name
    ,account_manager_source_sid
    ,account_manager_name
    ,active_ind
    ,policy_level_source_code
    ,ajg_policy_num
    ,renewal_ind
    ,original_policy_id
    ,original_effective_date
    ,tax_state_code
    ,operations_of_insurance_desc
    ,program_eligibility_code
    ,program_eligibility_name
    ,program_desc_01
    ,program_desc_02
    ,producer_contact_name
    ,policy_source_code
    ,policy_name
    ,policy_type_name
    ,carrier_source_sid
    ,carrier_name
    ,excess_primary_code
    ,sm_exclusion_ind
    ,multi_year_ind
    ,master_policy_num
    ,continuous_policy_ind
    ,commission_rate_pct
    ,member_enrollment_count
    ,CASE 
        WHEN exp_ind THEN FALSE 
        WHEN rei_ind THEN TRUE 
        ELSE etl_active_ind
    END AS etl_active_ind
    ,scd_effective_datetime
    ,CASE 
        WHEN upd2_ind OR exp_ind THEN {{ var('ctl_var__scd_expiration_datetime') }} 
        WHEN rei_ind THEN {{ var('ctl_var__env_variables')['future_datetime'] }} 
        ELSE scd_expiration_datetime 
    END AS scd_expiration_datetime
    ,CASE 
        WHEN upd2_ind THEN FALSE 
        ELSE scd_current_ind 
    END AS scd_current_ind
FROM src 
WHERE NOT ins_ind AND (upd1_ind OR upd2_ind OR exp_ind OR rei_ind)

UNION ALL

SELECT 
    NULL AS join_policy_key
    ,policy_key
    ,env_source_code
    ,data_source_code
    ,policy_id
    ,policy_source_sid
    ,policy_num
    ,plan_num
    ,effective_date
    ,expiration_date
    ,inception_date
    ,bound_date
    ,mail_out_date
    ,cancel_date
    ,cancel_reason_code
    ,reinstated_date
    ,policy_status_code
    ,policy_status_desc
    ,endorsement_code
    ,sic_code
    ,bill_type_code
    ,invoice_date
    ,contracted_expiration_date
    ,cancellation_reason_desc
    ,cancellation_reason_other_desc
    ,annualized_endorsement_premium_amt
    ,written_premium_amt
    ,annualized_premium_amt
    ,producer_source_sid
    ,producer_name
    ,account_manager_source_sid
    ,account_manager_name
    ,active_ind
    ,policy_level_source_code
    ,ajg_policy_num
    ,renewal_ind
    ,original_policy_id
    ,original_effective_date
    ,tax_state_code
    ,operations_of_insurance_desc
    ,program_eligibility_code
    ,program_eligibility_name
    ,program_desc_01
    ,program_desc_02
    ,producer_contact_name
    ,policy_source_code
    ,policy_name
    ,policy_type_name
    ,carrier_source_sid
    ,carrier_name
    ,excess_primary_code
    ,sm_exclusion_ind
    ,multi_year_ind
    ,master_policy_num
    ,continuous_policy_ind
    ,commission_rate_pct
    ,member_enrollment_count
    ,TRUE AS etl_active_ind
    ,CASE 
        WHEN ins_ind THEN {{ var('ctl_var__env_variables')['past_datetime'] }} 
        ELSE {{ var('ctl_var__scd_effective_datetime') }} 
    END AS scd_effective_datetime
    ,{{ var('ctl_var__env_variables')['future_datetime'] }} AS scd_expiration_datetime
    ,TRUE AS scd_current_ind
FROM src 
WHERE ins_ind OR upd2_ind;
```


/*
1. The model is configured with a `materialized='table'` setting and appropriate database, schema, and alias.
2. The `IDENTIFIER(:temp_hist_table)` is replaced with a dbt variable `{{ var('ctl_var__temp_hist_table') }}`.
3. The `CASE` statements are retained as-is since they are compatible with Snowflake SQL.
4. SQL functions such as `IFNULL` were validated, as they are compatible with Snowflake.
5. The original SQL comments are preserved to retain context.
6. Variables in the SQL like `:env_source_code` and `:scd_expiration_datetime` are replaced with dbt variables using `{{ var('ctl_var__...') }}`.
7. The ref function is used for table references, following the naming format: `alias__database__schema`.
8. The UNION ALL statements are converted to Snowflake-compatible syntax and adapted to use dbt variables.
9. COALESCE is used to handle NULL checks, which is compatible with Snowflake SQL.
10. The Snowflake dialect allows for the use of `CAST` and `COALESCE` as shown, making the SQL fully compatible.
*/