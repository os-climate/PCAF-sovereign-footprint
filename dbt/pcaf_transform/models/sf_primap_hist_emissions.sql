{{ config(materialized='view', view_security='invoker') }}
with source_data as (
    select rec_source, data_provider, country_name, country_iso_code, validity_date, attribute, value, value_units
    from osc_datacommons_dev.mdt_sandbox.sf_primap_hist_emissions_source
)
select * from source_data
