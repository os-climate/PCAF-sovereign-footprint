{{ config(materialized='view', view_security='invoker') }}
with source_data as (
    select attribute, country_iso_code, validity_date, value
    from osc_datacommons_dev.mdt_sandbox.sf_oecd_exch_rates_source
)
select * from source_data
