{{ config(materialized='view', view_security='invoker') }}
with source_data as (
    select attribute, country_iso_code, country_name, partner_iso_code, industry_code, industry_name, validity_date, value, value_units
    from osc_datacommons_dev.mdt_sandbox.sf_oecd_exgr_dco2_source
)
select * from source_data


