
from minio import Minio
from datetime import datetime
import os
from dbt.cli.main import dbtRunner
from airflow.decorators import dag, task


from io import BytesIO

def dynamic_task(model_name):
    @task(task_id=f"dbt_run__{model_name}")
    def dbt_run_model(model_name, manifest):
        dbt_runner = dbtRunner(manifest)
        result = dbt_runner.invoke(["run", "-s", model_name])

    return dbt_run_model

def dynamic_dbt_dag(project_path, project_name):
    @dag(
        dag_id=f"dbt_dag__{project_name}",
        schedule="0 * * * *",
        start_date=datetime(2024, 1, 1)
    )
    def dbt_dag():
        
        # access_key = os.environ["MINIO_ACCESS_KEY"]
        # secret_key = os.environ["MINIO_SECRET_KEY"]
        access_key = "kDZHuGN0Wwa3DzSA84rZ"
        secret_key = "F6w8XW60S816HFY01wqx9Wxyn8Cydi2igcCw7pR1"
        
        bucket_name = "airflow-metadata"

        client = Minio(
            endpoint="192.168.0.147:30091",
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        object_name = f"manifest__{project_name}"

        # Parsing and storing dbt manifests

        print("pasing project: ", project_path)

        dbt_runner = dbtRunner()
        result = dbt_runner.invoke(["parse", "--project-dir", project_path])

        if not result.success:
            print(result.exception)
            exit(1)

        client.put_object(
            bucket_name,
            object_name,
            BytesIO(result.result.to_msgpack()),
            length=-1,
            part_size=10*1024*1024
        )

        for name, node in result.result.nodes.items():
            if name.startswith("model."):
                model_exec = dynamic_task(name)(name, result.result)

                deps = [
                    dynamic_task(dep)(dep, manifest=result.result)
                    for dep in node.depends_on.nodes
                    if dep.startswith("model.")
                ]
                if deps:
                    model_exec >> deps
                      

    dbt_dag()
        