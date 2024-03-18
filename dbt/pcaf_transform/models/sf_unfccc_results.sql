{{ config(materialized='view', view_security='invoker') }}
with source_data as (
    select country_iso_code, country_name, validity_date, ghg_total_with_lulucf, ghg_total_with_lulucf_units, ghg_total_without_lulucf, ghg_total_without_lulucf_units, scope1_excl_source, gdp, gdp_units, gdp_ppp, gdp_ppp_units, ghg_intensity_with_lulucf_per_gdp, ghg_intensity_with_lulucf_per_gdp_units, ghg_intensity_without_lulucf_per_gdp, ghg_intensity_without_lulucf_per_gdp_units
    from osc_datacommons_dev.mdt_sandbox.sf_unfccc_results_source
)
select * from source_data
