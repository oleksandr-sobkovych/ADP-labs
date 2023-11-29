from __future__ import annotations
import json

from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance

from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

city_to_coords = {
    "Lviv": {"lat": "49.841952", "lon": "24.0315921"},
    "Kyiv": {"lat": "50.4500336", "lon": "30.5241361"},
    "Kharkiv": {"lat": "49.9923181", "lon": "36.2310146"},
    "Odesa": {"lat": "46.4843023", "lon": "30.7322878"},
    "Zhmerynka": {"lat": "49.0354593", "lon": "28.1147317"},
}


def _process_weather(ti: TaskInstance):
    cities = list(city_to_coords.keys())
    cities_info = list(
        map(
            lambda i: i["data"][0],
            ti.xcom_pull([f"{city}_extract_data" for city in cities]),
        )
    )

    return {
        city: [
            info["dt"],
            info["temp"],
            info["humidity"],
            info["clouds"],
            info["wind_speed"],
        ]
        for city in cities
        for info in cities_info
    }


with DAG(
    dag_id="weather_dag",
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=True,
) as dag:
    ts = "{{ dag_run.execution_date.timestamp() | int }}"
    weather_key = Variable.get("WEATHER_API_KEY")

    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
CREATE TABLE IF NOT EXISTS measures
(
timestamp TIMESTAMP,
city TEXT,
temp FLOAT,
humidity INTEGER,
clouds INTEGER,
wind_speed FLOAT
);""",
    )

    with TaskGroup(group_id="extract_cities", prefix_group_id=False) as extract_cities:
        for city in city_to_coords:
            request_params = {"appid": weather_key, **city_to_coords[city], "dt": ts}

            check_api = HttpSensor(
                task_id=f"{city}_check_api",
                http_conn_id="weather_conn",
                endpoint="data/3.0/onecall/timemachine",
                request_params=request_params,
            )

            extract_data = SimpleHttpOperator(
                task_id=f"{city}_extract_data",
                http_conn_id="weather_conn",
                endpoint="data/3.0/onecall/timemachine",
                data=request_params,
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True,
            )

            check_api >> extract_data

    process_data = PythonOperator(
        task_id="process_data", python_callable=_process_weather
    )

    store_cities = [
        SqliteOperator(
            task_id=f"{city}_inject_data",
            sqlite_conn_id="airflow_conn",
            sql=f"""
INSERT INTO measures (timestamp, city, temp, humidity, clouds, wind_speed) VALUES
({{{{ti.xcom_pull(task_ids='process_data')['{city}'][0]}}}},
'{city}',
{{{{ti.xcom_pull(task_ids='process_data')['{city}'][1]}}}},
{{{{ti.xcom_pull(task_ids='process_data')['{city}'][2]}}}},
{{{{ti.xcom_pull(task_ids='process_data')['{city}'][3]}}}},
{{{{ti.xcom_pull(task_ids='process_data')['{city}'][4]}}}});
""",
        )
        for city in city_to_coords.keys()
    ]

    db_create >> extract_cities >> process_data
    process_data >> store_cities
