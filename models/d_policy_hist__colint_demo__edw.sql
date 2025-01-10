-- d_policy_hist__colint_demo__edw.sql
-- SQL Object conversion metadata
-- conversion_id: 3e0e04da09dce8eb7b56351a531fe454
-- group_id: ec822ec95dd84501b49995293d240a28
-- dbt_spoc_converter_sql_model_version: 1.2.0
-- model_conversion_started_at: 2024-12-20T17:12:49.264606+00:00
-- Source Statement SQL metadata
-- source_id: 7b9781e37167d3031d7d911e5f09499f

{{
    config(
        materialized='incremental',
        database='colint_demo',
        schema='edw',
        alias='d_policy_hist',
        unique_key=['policy_key'],
        incremental_strategy='merge',
        meta={
            'input_sql_dialect': 'Snowflake SQL',
            'input_stored_procedure_runner': 'Azure Data Factory',
            'output_sql_dialect': 'Snowflake',
            'source_file_name': 'R__sp\\_*load*\\_d*policy*hist.sql'
        }
    )
}}

WITH source_data AS (
    SELECT
        policy_key
        ,{{ var('ctl_var__etl_process_run_id') }} AS etl_process_run_id
        ,{{ var('ctl_var__etl_process_name') }} AS etl_process_name
        ,CURRENT_TIMESTAMP() AS etl_create_datetime
        ,NULL AS etl_update_datetime
        ,scd_effective_datetime
        ,scd_expiration_datetime
        ,scd_current_ind
        ,etl_active_ind
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
    FROM {{ var('ctl_var__temp_hist_table') }}
)

SELECT
    t.policy_key
    ,{{ var('ctl_var__etl_process_run_id') }} AS etl_process_run_id
    ,{{ var('ctl_var__etl_process_name') }} AS etl_process_name
    ,CURRENT_TIMESTAMP() AS etl_update_datetime
    ,IFF(t.scd_current_ind, s.scd_expiration_datetime, t.scd_expiration_datetime) AS scd_expiration_datetime
    ,IFF(t.scd_current_ind, s.scd_current_ind, t.scd_current_ind) AS scd_current_ind
    ,s.etl_active_ind
    ,s.policy_source_sid
    ,s.plan_num
    ,s.inception_date
    ,s.bound_date
    ,s.mail_out_date
    ,s.reinstated_date
    ,s.invoice_date
    ,s.account_manager_source_sid
    ,s.account_manager_name
    ,s.active_ind
    ,s.policy_level_source_code
    ,s.original_policy_id
    ,s.original_effective_date
    ,s.tax_state_code
    ,s.operations_of_insurance_desc
    ,s.producer_contact_name
    ,s.policy_source_code
    ,s.policy_name
    ,s.policy_type_name
    ,s.carrier_source_sid
    ,s.carrier_name
    ,s.excess_primary_code
    ,s.sm_exclusion_ind
    ,s.master_policy_num
    ,s.continuous_policy_ind
    ,s.commission_rate_pct
    ,s.member_enrollment_count
FROM {{ this }} AS t
RIGHT JOIN source_data AS s
ON t.policy_key = s.policy_key

/* 
Conversion Logic:
1. Config block includes the merge strategy with the unique_key set to 'policy_key'.
2. Variables are replaced using the dbt `var` function for dynamic substitution.
3. The original Snowflake SQL MERGE is emulated using a RIGHT JOIN to handle updates and insertions.
4. Current timestamps are used for `etl_create_datetime` and `etl_update_datetime`.
5. The `IFF` function handles conditional logic for `scd_expiration_datetime` and `scd_current_ind`.
6. The `source_data` CTE is used to transform the temporary historical table data.
*/

