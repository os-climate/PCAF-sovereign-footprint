import os
import pathlib
import json
import osc_ingest_trino as osc
import trino
from sqlalchemy.engine import create_engine

import pandas as pd
from openscm_units import unit_registry
from pint import set_application_registry, Quantity
from pint_pandas import PintArray, PintType

# First we create the registry.
ureg = unit_registry
Q_ = ureg.Quantity
ureg.default_format = "~"
ureg.define("CO2e = CO2 = CO2eq")
ureg.define("USD = [currency]")
ureg.define("EUR = [currency_EUR]")
ureg.define("Millions=1000000")
set_application_registry(ureg)

# Load environment variables from credentials.env
osc.load_credentials_dotenv()

import boto3

s3_source = boto3.resource(
    service_name="s3",
    endpoint_url=os.environ["S3_LANDING_ENDPOINT"],
    aws_access_key_id=os.environ["S3_LANDING_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_LANDING_SECRET_KEY"],
)
source_bucket = s3_source.Bucket(os.environ["S3_LANDING_BUCKET"])

hive_catalog = "osc_datacommons_hive_ingest"
hive_schema = "ingest"
hive_bucket = osc.attach_s3_bucket("S3_HIVE")

ingest_catalog = "osc_datacommons_dev"
ingest_schema = "mdt_sandbox"
pcaf_table_prefix = ""
trino_bucket = osc.attach_s3_bucket("S3_DEV")

engine = osc.attach_trino_engine(verbose=True, catalog=ingest_catalog)

# make sure schema exists, or table creation below will fail in weird ways
qres = osc._do_sql(
    f"create schema if not exists {ingest_catalog}.{ingest_schema}",
    engine,
    verbose=True,
)


def requantify_df(df, typemap={}):
    units_col = None
    columns_not_found = [k for k in typemap.keys() if k not in df.columns]
    if columns_not_found:
        print(f"columns {columns_not_found} not found in DataFrame")
        raise ValueError
    columns_reversed = reversed(df.columns)
    for col in columns_reversed:
        if col.endswith("_units"):
            if units_col:
                print(
                    f"Column {units_col} follows {col} without intervening value column"
                )
                # We expect _units column to follow a non-units column
                raise ValueError
            units_col = col
            continue
        if units_col:
            if col + "_units" != units_col:
                print(f"Excpecting column name {col}_units but saw {units_col} instead")
                raise ValueError
            if (df[units_col] == df[units_col].iloc[0]).all():
                # We can make a PintArray since column is of homogeneous type
                # ...and if the first valid index matches all, we can take first row as good
                new_col = PintArray(df[col], dtype=f"pint[{df[units_col].iloc[0]}]")
            else:
                # Make a pd.Series of Quantity in a way that does not throw UnitStrippedWarning
                new_col = pd.Series(data=df[col], name=col) * pd.Series(
                    data=df[units_col].map(
                        lambda x: (
                            typemap.get(col, "dimensionless")
                            if pd.isna(x)
                            else ureg(x).u
                        )
                    ),
                    name=col,
                )
            if col in typemap.keys():
                new_col = new_col.astype(f"pint[{typemap[col]}]")
            df = df.drop(columns=units_col)
            df[col] = new_col
            units_col = None
        elif col in typemap.keys():
            df[col] = df[col].astype(f"pint[{typemap[col]}]")
    return df


# If DF_COL contains Pint quantities (because it is a PintArray or an array of Pint Quantities),
# return a two-column dataframe of magnitudes and units.
# If DF_COL contains no Pint quanities, return it unchanged.


def dequantify_column(df_col: pd.Series):
    if type(df_col.values) == PintArray:
        return pd.DataFrame(
            {
                df_col.name: df_col.values.quantity.m,
                df_col.name + "_units": str(df_col.values.dtype.units),
            },
            index=df_col.index,
        )
    elif df_col.size == 0:
        return df_col
    elif isinstance(df_col.iloc[0], Quantity):
        m, u = list(
            zip(
                *df_col.map(
                    lambda x: (
                        (np.nan, "dimensionless") if pd.isna(x) else (x.m, str(x.u))
                    )
                )
            )
        )
        return pd.DataFrame(
            {df_col.name: m, df_col.name + "_units": u}, index=df_col.index
        )
    else:
        return df_col


# Rewrite dataframe DF so that columns containing Pint quantities are represented by a column for the Magnitude and column for the Units.
# The magnitude column retains the original column name and the units column is renamed with a _units suffix.
def dequantify_df(df):
    return pd.concat([dequantify_column(df[col]) for col in df.columns], axis=1)


# When reading SQL tables to import into DataFrames, it is up to the user to preserve {COL}, {COL}_units pairings so they can be reconstructed.
# If the user does a naive "select * from ..." this happens naturally.
# We can give a warning when we see a resulting dataframe that could have, but does not have, unit information properly integrated.  But
# fixing the query on the fly becomes difficult when we consider the fully complexity of parsing and rewriting SQL queries to put the units columns in the correct locations.
# (i.e., properly in the principal SELECT clause (which can have arbitrarily complex terms), not confused by FROM, WHERE, GROUP BY, ORDER BY, etc.)


def read_quantified_sql(sql, tablename, schemaname, engine, index_col=None):
    qres = osc._do_sql(f"describe {schemaname}.{tablename}", engine, verbose=False)
    # tabledesc will be a list of tuples (column, type, extra, comment)
    colnames = [x[0] for x in qres]
    # read columns normally...this will be missing any unit-related information
    sql_df = pd.read_sql(sql, engine, index_col)
    # if the query requests columns that don't otherwise bring unit information along with them, get that information too
    extra_unit_columns = [
        (i, f"{col}_units")
        for i, col in enumerate(sql_df.columns)
        if f"{col}_units" not in sql_df.columns and f"{col}_units" in colnames
    ]
    if extra_unit_columns:
        extra_unit_columns_positions = [
            (i, extra_unit_columns[i][0], extra_unit_columns[i][1])
            for i in range(len(extra_unit_columns))
        ]
        for col_tuple in extra_unit_columns_positions:
            print(
                f"Missing units column '{col_tuple[2]}' after original column '{sql_df.columns[col_tuple[1]]}' (should be column #{col_tuple[0]+col_tuple[1]+1} in new query)"
            )
        raise ValueError
    else:
        return requantify_df(sql_df).convert_dtypes()


HOMEDIR = os.getcwd().rsplit("/", 1)[0]
DBT_DIR = "pcaf_transform"

# The following text describes DBT model properties

"""
version: 2

models:
  - [name](model_name): <model name>
    [description](description): <markdown_string>
    [docs](resource-properties/docs):
      show: true | false
    [config](resource-properties/config):
      [<model_config>](model-configs): <config_value>
    [tests](resource-properties/tests):
      - <test>
      - ... # declare additional tests
    columns:
      - name: <column_name> # required
        [description](description): <markdown_string>
        [meta](meta): {<dictionary>}
        [quote](quote): true | false
        [tests](resource-properties/tests):
          - <test>
          - ... # declare additional tests
        [tags](resource-configs/tags): [<string>]

      - name: ... # declare properties of additional columns
"""

# The following text describes DBT external properties

"""
version: 2

sources:
  - name: <source_name>
    tables:
      - name: <table_name>
        external:
          location: <string>
          file_format: <string>
          row_format: <string>
          tbl_properties: <string>
          partitions:
            - name: <column_name>
              data_type: <string>
              description: <string>
              meta: {dictionary}
            - ...
          <additional_property>: <additional_value>
"""

dbt_dict = {}
dbt_dict["models"] = {}


def create_trino_table_and_dbt_metadata(
    tablename,
    df,
    partition_columns=[],
    custom_meta_content="",
    custom_meta_fields="",
    verbose=False,
):
    ingest_table = f"{pcaf_table_prefix}{tablename}"

    if custom_meta_content:
        dbt_models = dbt_dict["models"]
        dbt_models[ingest_table] = dbt_table = {
            "description": custom_meta_content["description"]
        }
        if custom_meta_fields:
            dbt_table["columns"] = dbt_columns = {
                name: {"description": custom_meta_fields[name]["Description"]}
                for name in custom_meta_fields.keys()
            }
            for name in custom_meta_fields.keys():
                if "tags" in custom_meta_fields[name].keys():
                    dbt_columns[name]["tags"] = custom_meta_fields[name]["tags"]
    elif custom_meta_fields:
        raise ValueError

    drop_table = osc._do_sql(
        f"drop table if exists {ingest_schema}.{ingest_table}_source",
        engine,
        verbose=verbose,
    )

    osc.fast_pandas_ingest_via_hive(
        df,
        engine,
        ingest_catalog,
        ingest_schema,
        f"{ingest_table}_source",
        hive_bucket,
        hive_catalog,
        hive_schema,
        partition_columns=partition_columns,
        overwrite=True,
        typemap={"datetime64[ns]": "date"},
        verbose=verbose,
    )

    with open(
        f"{HOMEDIR}/dbt/{DBT_DIR}/models/{ingest_table}.sql", "w", encoding="utf-8"
    ) as f:
        print(
            "{{ config(materialized='view', view_security='invoker') }}"
            + f"""
with source_data as (
    select {', '.join(df.columns)}
    from {ingest_catalog}.{ingest_schema}.{ingest_table}_source
)
select * from source_data

""",
            file=f,
        )
