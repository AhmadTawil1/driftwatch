"""Confirms the DAG folder is wired up correctly. Not part of the pipeline
— safe to delete once a real DAG exists in day 3."""

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(dag_id="smoke_test", schedule=None, catchup=False):
    EmptyOperator(task_id="noop")
