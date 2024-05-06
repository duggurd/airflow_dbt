from dbt.cli.main import dbtRunner


dbt_runner = dbtRunner()
result = dbt_runner.invoke(["parse", "--project-dir", "./testing"])
