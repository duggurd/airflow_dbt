from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="dummy_dag",
    schedule="0 * * * *",
    start_date=datetime(2024, 1 ,1)
)
def some_dag():
    @task
    def some_task():
        print("this is a tasl")

    some_task()

some_dag()