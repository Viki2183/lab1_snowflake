# Lab 1 – Weather Prediction Analytics

## Technologies Used
- Apache Airflow
- Snowflake
- Open-Meteo API
- Python
- Scikit-Learn

## DAGs
1. open_meteo_etl
2. weather_forecast_ml

## Tables
- OPEN_METEO_RAW
- WEATHER_OBSERVATION_HOURLY
- FORECAST_OUTPUT
- WEATHER_FINAL

## Execution Flow
Open-Meteo API → Snowflake RAW → Transform → ML Forecast → Final Union Table