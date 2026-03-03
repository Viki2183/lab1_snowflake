from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime
import requests
import json
import uuid

SNOWFLAKE_CONN_ID = "snowflake_lab"

# -----------------------------
# 1️⃣ Extract & Load RAW JSON
# -----------------------------
def extract_and_load():
    locations = json.loads(Variable.get("open_meteo_locations"))
    hourly_fields = Variable.get("open_meteo_hourly_fields")
    timezone = Variable.get("open_meteo_timezone")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Always explicitly set context
    hook.run("USE WAREHOUSE chipmunk_wh")
    hook.run("USE DATABASE user_db_chipmunk")
    hook.run("USE SCHEMA SCHEMA_WEATHER")

    for loc in locations:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": loc["lat"],
                "longitude": loc["lon"],
                "hourly": hourly_fields,
                "timezone": timezone
            }
        )

        data = response.json()

        insert_sql = """
        INSERT INTO OPEN_METEO_RAW
        (INGEST_ID, INGEST_TS_UTC, LAT, LON, API_URL, RESPONSE_JSON)
        SELECT %s,
               CURRENT_TIMESTAMP(),
               %s,
               %s,
               %s,
               TO_VARIANT(PARSE_JSON(%s))
        """

        hook.run(
            insert_sql,
            parameters=(
                str(uuid.uuid4()),
                loc["lat"],
                loc["lon"],
                response.url,
                json.dumps(data)
            )
        )

# -----------------------------
# 2️⃣ Transform RAW → Hourly
# -----------------------------
def transform_hourly():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Explicit context (prevents future errors)
    hook.run("USE WAREHOUSE chipmunk_wh")
    hook.run("USE DATABASE user_db_chipmunk")
    hook.run("USE SCHEMA SCHEMA_WEATHER")

    transform_sql = """
    INSERT INTO WEATHER_OBSERVATION_HOURLY
    SELECT
        TO_TIMESTAMP_NTZ(t.value::string) AS OBS_TS_UTC,
        r.LAT,
        r.LON,
        r.RESPONSE_JSON:hourly:temperature_2m[t.index]::float,
        r.RESPONSE_JSON:hourly:relative_humidity_2m[t.index]::float,
        r.RESPONSE_JSON:hourly:wind_speed_10m[t.index]::float,
        r.RESPONSE_JSON:hourly:precipitation[t.index]::float,
        CURRENT_TIMESTAMP() AS LOAD_TS_UTC
    FROM OPEN_METEO_RAW r,
         LATERAL FLATTEN(input => r.RESPONSE_JSON:hourly:time) t;
    """

    hook.run(transform_sql)

# -----------------------------
# 3️⃣ DAG Definition
# -----------------------------
with DAG(
    dag_id="open_meteo_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load
    )

    t2 = PythonOperator(
        task_id="transform_hourly",
        python_callable=transform_hourly
    )

    t1 >> t2