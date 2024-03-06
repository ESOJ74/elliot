from airflow import DAG
from datetime import datetime
import time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
import psycopg2
from airflow.models.baseoperator import chain


def fetch_data(sql_query):
    conn = psycopg2.connect(dbname='opdenergy', user='opdenergy', password='tIF3J3tgIZRnvgHaVCD0',
                        host='opdenergy-postgres.elliotcloud.com', port=5432, application_name='weatherbit-forecast')
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        conn.commit()

    finally:
        cursor.close()
        conn.close()
    return result

sql = "SELECT readi_id FROM plants WHERE tecnologia = 'Wind';"
result = fetch_data(sql)
plants = [tup[0] for tup in result]

dag = DAG('meteorology-openmeteo-wind',
          default_args={
          "max_active_runs": 1,
          "pool": "meteo"
          },
          start_date=datetime(2024, 2, 20),
          schedule_interval='03 * * * *',
          catchup=True,
          params={}
          )

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

parallel_tasks = [HttpSensor(
    task_id=f"omdata_{id_plant}",
    http_conn_id="openmeteo",
    endpoint="openmeteo",
    request_params={"date": "{{ data_interval_end }}",
                        "plant": f"{id_plant}",
                        "freq": "10M"
                        },
    response_check=lambda response: "ok" in response.text,
    dag=dag,
) for id_plant in plants]


start >> parallel_tasks >> end
