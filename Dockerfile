FROM python:3.11-slim-bullseye
RUN pip install apache-airflow==2.8.2 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.11.txt
RUN pip install dbt-core dbt-postgres minio 
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV DBT_PROFILES_DIR=/root/airflow/dbt
USER root
ENTRYPOINT [ "airflow" ]