# PCAF Sovereign Footprint

## Project Description

PCAF have defined Scope 1, Scope 2 and Scope 3 carbon emissions for Sovereigns in analogy to the GHG Protocol’s definition for Community-Scale Greenhouse Gas Inventories (Cities):

- **Scope 1:** Domestic GHG emissions from sources located within the country territory. This aligns with the UNFCCC definition of domestic territorial emissions, including emissions from exported goods and services. (Production Emissions). Separate reporting incl./excl. land use, land use-change and forestry (LULUCF).

- **Scope 2:** GHG emissions occurring as a consequence of the domestic use of grid-supplied electricity, heat, steam and/or cooling which is imported from another territory

- **Scope 3:** Emissions attributable to non-energy imports as a result of activities taking place within the country territory

## Definitions

- **Consumption Emissions:** From these definitions PCAF derive Consumption Emissions as Scope 1 + Scope 2 + Scope 3 – emissions related to exports of goods and services

- **Intensity measures:** Intensity measures are defined for production emissions per GDP and Purchasing Power Parity adjusted GDP. For consumption emissions the analogous intensity metric is per capita.

## Sources

- **Scope 1:** UNFCCC Annex I including LULUCF and excluding LULUCF taken from <https://di.unfccc.int/time_series>

- **Scope 2 and 3:** Query based on “My Queries” Tool in <https://stats.oecd.org/Index.aspx?DataSetCode=IO_GHG_2021>

- **GDP and PPP adj. GDP:** <https://data.worldbank.org/indicator/NY.GDP.MKTP.CD>

## Implementation

- Amazon S3 to store the source files
- JupyterHub used to edit and execute the notebooks
- Github Link for source code: <https://github.com/os-climate/PCAF-sovereign-footprint.git>
- **Used Python Libraries:**

1. Pandas to read and transform Excel and CSV files
2. Pint to deal with the units
3. Pycountry and Country-converter to map the country names with the ISO codes

- **Trino** as query engine and object storage

## Author

Mutlu Ersin

- [Profile](https://github.com/mersin35)
