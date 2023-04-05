import pandas as pd
import json


def model(dbt, session):
    dbt.config(materialized="table")
    dbt.config(schema="SENSOR")
    dbt.config(database="TAKE_HOME_USER2")
    dbt.config(packages=["pandas"])
    raw_df = session.table("SENSOR.TIMESERIES")
    raw_pdf = raw_df.to_pandas()
    # parse the json string, normalize
    normalized_pdf = pd.json_normalize(raw_pdf["V"].apply(json.loads))
    readings_pdf = pd.json_normalize(normalized_pdf["data.data"].explode())
    return readings_pdf
    # 21:07:20    391529 (42501): SQL compilation error: Anaconda terms must be accepted by ORGADMIN to use Anaconda 3rd party packages. Please follow the instructions at https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#using-third-party-packages-from-anaconda.
