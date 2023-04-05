from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION
import json
import pandas as pd
from datetime import datetime, timedelta


# we are crashing the jupyter kernel now...
def connect_to_snowflake():
    # Create Snowflake Session object
    print("Connecting to Snowflake...")
    connection_parameters = json.load(open("connection.json"))
    session = Session.builder.configs(connection_parameters).create()

    # Current Environment
    print("Current Environment...")
    snowflake_environment = session.sql(
        "select current_user(), current_role(), current_database(), current_schema(), current_version(), current_warehouse()"
    ).collect()
    snowpark_version = VERSION
    print("   User                        : {}".format(snowflake_environment[0][0]))
    print("   Role                        : {}".format(snowflake_environment[0][1]))
    print("   Database                    : {}".format(snowflake_environment[0][2]))
    print("   Schema                      : {}".format(snowflake_environment[0][3]))
    print("   Warehouse                   : {}".format(snowflake_environment[0][5]))
    print("   Snowflake version           : {}".format(snowflake_environment[0][4]))
    print(
        "   Snowpark for Python version : {}.{}.{}".format(
            snowpark_version[0], snowpark_version[1], snowpark_version[2]
        )
    )
    return session


session = connect_to_snowflake()
sensor_df = session.table("SENSOR.TIMESERIES")
sensor_pdf = sensor_df.to_pandas()
normalized_pdf = pd.json_normalize(sensor_pdf["V"].apply(json.loads))
normalized_readings_pdf = pd.json_normalize(normalized_pdf["data.data"].explode())
session.write_pandas(
    normalized_readings_pdf,
    table_name="TIMESERIES_NORMAL",
    database="TAKE_HOME_USER2",
    schema="SENSOR",
)
