from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression

def train_and_forecast():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_lab")
    hook.run("USE WAREHOUSE chipmunk_wh")

    df = hook.get_pandas_df("""
        SELECT OBS_TS_UTC, LAT, LON, TEMP_C
        FROM WEATHER_OBSERVATION_HOURLY
        WHERE TEMP_C IS NOT NULL
        ORDER BY OBS_TS_UTC
    """)

    df["NEXT_TEMP"] = df["TEMP_C"].shift(-1)
    df = df.dropna()

    model = LinearRegression()
    X = df[["TEMP_C"]]
    y = df["NEXT_TEMP"]
    model.fit(X, y)

    last_row = df.iloc[-1:]
    prediction = model.predict(last_row[["TEMP_C"]])[0]

    hook.run("""
        INSERT INTO FORECAST_OUTPUT
        VALUES (CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), %s, %s, %s, 'LR', 'v1', CURRENT_TIMESTAMP())
    """, parameters=(last_row["LAT"].values[0],
                     last_row["LON"].values[0],
                     float(prediction)))

def final_union_merge():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_lab")
    hook.run("USE WAREHOUSE chipmunk_wh")

    try:
        hook.run("BEGIN")

        hook.run("""
        INSERT INTO WEATHER_FINAL
        SELECT OBS_TS_UTC, LAT, LON, TEMP_C, NULL, 'OBS', CURRENT_TIMESTAMP()
        FROM WEATHER_OBSERVATION_HOURLY

        UNION ALL

        SELECT FORECAST_TS_UTC, LAT, LON, NULL, PRED_TEMP_C, 'FCST', CURRENT_TIMESTAMP()
        FROM FORECAST_OUTPUT;
        """)

        hook.run("COMMIT")
    except:
        hook.run("ROLLBACK")
        raise

with DAG(
    dag_id="weather_forecast_ml",
    start_date=datetime(2024,1,1),
    schedule="@hourly",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="train_and_forecast",
        python_callable=train_and_forecast
    )

    t2 = PythonOperator(
        task_id="final_union_merge",
        python_callable=final_union_merge
    )

    t1 >> t2