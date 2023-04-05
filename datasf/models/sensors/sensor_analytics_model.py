def model(dbt, session):
    # dbt.config(materialized="table")

    df = dbt.ref("sensor_data_normalize_model")
    return df
