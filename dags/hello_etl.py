
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_github_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "github"],
)
def hello_github_dag():

    @task
    def extract():
        print("Step 1: Extracting data")
        return {"records": 100}

    @task
    def transform(data):
        print(f"Step 2: Processing {data['records']} records")
        return data["records"]

    @task
    def load(records):
        print(f"Step 3: Loaded {records} records successfully")

    data = extract()
    records = transform(data)
    load(records)


hello_github_dag()
