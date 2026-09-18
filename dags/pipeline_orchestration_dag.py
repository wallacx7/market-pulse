"""DAG do Airflow: orquestra dbt build -> agente de diagnóstico -> notificação.

A ingestão (ingestion/) roda continuamente fora deste DAG (processo de longa
duração, não um job agendado). Este DAG cuida da parte batch, agendada:
transformar o que foi ingerido, checar qualidade e diagnosticar anomalias.

Ainda não implementado — depende de dbt/ e agent/ estarem funcionais.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from agent.agent import diagnose_failures

default_args = {
    "owner": "market_pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_pulse_pipeline",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    run_dbt = BashOperator(
        task_id="dbt_build",
        bash_command="cd dbt && dbt build",
    )

    run_agent = PythonOperator(
        task_id="diagnose_anomalies",
        python_callable=diagnose_failures,
    )

    run_dbt >> run_agent
