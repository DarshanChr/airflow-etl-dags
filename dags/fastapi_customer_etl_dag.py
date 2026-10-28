
from datetime import datetime
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

from airflow import DAG
from airflow.operators.python import PythonOperator


FASTAPI_URL = (
    "http://fast-api-etl.default.svc.cluster.local:8000/extract"
)


# Step 1: Start message
def start_etl():
    print("Starting customer ETL pipeline...")
    print("Preparing to call FastAPI endpoint.")


# Step 2: Call FastAPI GET endpoint
def call_fastapi():
    print(f"Calling FastAPI endpoint: {FASTAPI_URL}")

    try:
        with urlopen(FASTAPI_URL, timeout=600) as response:
            result = response.read().decode("utf-8")

            print(f"HTTP Status: {response.status}")
            print(f"API Response: {result}")

            if not 200 <= response.status < 300:
                raise RuntimeError(
                    f"FastAPI returned status {response.status}"
                )

    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"FastAPI failed with HTTP {exc.code}: {body}"
        ) from exc

    except URLError as exc:
        raise RuntimeError(
            f"Unable to connect to FastAPI: {exc}"
        ) from exc

    print("FastAPI ETL request completed successfully.")


# Step 3: Success message
def etl_success():
    print("Customer ETL pipeline completed successfully!")
    print("Check PostgreSQL table: customer_etl_target")


with DAG(
    dag_id="fastapi_customer_etl",
    description="Trigger customer ETL using FastAPI",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
    tags=["fastapi", "etl"],
) as dag:

    start_task = PythonOperator(
        task_id="start_etl",
        python_callable=start_etl,
    )

    call_api_task = PythonOperator(
        task_id="call_fastapi_api",
        python_callable=call_fastapi,
    )

    success_task = PythonOperator(
        task_id="etl_success",
        python_callable=etl_success,
    )

    # Define execution order
    start_task >> call_api_task >> success_task
