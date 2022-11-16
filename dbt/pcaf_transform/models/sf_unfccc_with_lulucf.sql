{{ config(materialized='view', view_security='invoker') }}
with source_data as (
    select rec_source, data_provider, country_iso_code, country_name, validity_date, attribute, value, value_units
    from osc_datacommons_dev.mdt_sandbox.sf_unfccc_with_lulucf_source
)
select * from source_data


