# 🌦 Lab 1 – Weather Prediction Analytics using Snowflake & Airflow

## 📌 Overview

This project implements an end-to-end weather data analytics and forecasting pipeline using **Apache Airflow**, **Snowflake**, and the **Open-Meteo API**.

The system automatically:

- Retrieves 90 days of historical weather data for multiple locations  
- Stores raw API responses in Snowflake  
- Transforms JSON data into structured hourly and daily tables  
- Trains a Linear Regression model to forecast future temperatures  
- Merges historical and forecasted data using a SQL transaction  

The final dataset enables combined analysis of observed and predicted weather metrics.

---

## 🛠 Technologies Used

- Apache Airflow – Workflow orchestration  
- Snowflake – Cloud Data Warehouse  
- Open-Meteo API – Weather data source  
- Python (Scikit-Learn) – Machine learning forecasting  
- Docker – Containerized Airflow environment  

---

## 📍 Supported Locations

The pipeline supports multiple cities (minimum two):

- San Jose, CA  
- Cupertino, CA  

Locations are configured using Airflow Variables.

---

## 🔄 Airflow DAGs

### 1️⃣ open_meteo_etl

Performs data ingestion and transformation:

- Extracts 90 days of historical weather data  
- Loads raw JSON into `OPEN_METEO_RAW`  
- Transforms hourly data into `WEATHER_OBSERVATION_HOURLY`  
- Aggregates daily metrics into `WEATHER_DAILY`  

---

### 2️⃣ weather_forecast_ml

Performs machine learning forecasting:

- Trains a Linear Regression model using daily temperature data  
- Stores predictions in `FORECAST_OUTPUT`  
- Executes a SQL transaction to merge historical and forecast data into `WEATHER_FINAL`  

---

## 🗄 Database Tables

### 1️⃣ OPEN_METEO_RAW  
Stores raw JSON API responses.

### 2️⃣ WEATHER_OBSERVATION_HOURLY  
Stores structured hourly weather metrics.

### 3️⃣ WEATHER_DAILY  
Stores daily aggregated metrics:
- temp_max  
- temp_min  
- temp_mean  

### 4️⃣ FORECAST_OUTPUT  
Stores predicted temperature values.

### 5️⃣ WEATHER_FINAL  
Stores merged historical and forecast data using a SQL transaction.

---

## 🔁 Execution Flow

Open-Meteo API  
↓  
Airflow ETL DAG  
↓  
OPEN_METEO_RAW  
↓  
WEATHER_OBSERVATION_HOURLY  
↓  
WEATHER_DAILY  
↓  
ML Forecast DAG  
↓  
FORECAST_OUTPUT  
↓  
SQL Transaction (BEGIN / COMMIT)  
↓  
WEATHER_FINAL  

---

## 🔐 Airflow Configuration

### Variables Used
- `open_meteo_locations`
- `open_meteo_hourly_fields`
- `open_meteo_timezone`

### Snowflake Connection
- Connection ID: `snowflake_lab`
- Warehouse: `chipmunk_wh`
- Database: `user_db_chipmunk`
- Schema: `SCHEMA_WEATHER`

---

## 🧠 Key Concepts Demonstrated

- JSON parsing in Snowflake using VARIANT  
- LATERAL FLATTEN for nested arrays  
- ETL orchestration with Airflow  
- SQL transactions (BEGIN / COMMIT / ROLLBACK)  
- Machine learning forecasting integration  
- Multi-location pipeline design  

---

## 📊 Validation Query

```sql
SELECT RECORD_TYPE, COUNT(*)
FROM WEATHER_FINAL
GROUP BY RECORD_TYPE;