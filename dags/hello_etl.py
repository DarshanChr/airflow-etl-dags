from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("Hello from Airflow running in Minikube!")


with DAG(
    dag_id="hello_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "etl"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )