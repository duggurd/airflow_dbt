from pathlib import Path

from dbt_dag import dynamic_dbt_dag

dbt_root = Path("/root/airflow/dbt")

for project in dbt_root.iterdir():
    if project.is_dir():

        project_path = project.absolute()
        project_name = project.name
        
        dynamic_dbt_dag(project_path, project_name)
        