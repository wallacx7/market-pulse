"""Ferramentas que o agente de diagnóstico usa para investigar anomalias.

Cada função aqui é uma "tool" exposta ao agente: ele decide quando chamá-las
com base no que precisa investigar (ex: viu um teste dbt falho e quer saber
o histórico de preço do ativo no mesmo período).

Ainda não implementado — depende da tabela Gold estar populada no BigQuery
(etapas de ingestão + Databricks + dbt). Ver README para ordem de construção.
"""
from google.cloud import bigquery

from agent.config import GCP_PROJECT, GOLD_DATASET

BQ_CLIENT = bigquery.Client()


def query_failed_tests(run_id: str | None = None) -> list[dict]:
    """Retorna os testes dbt que falharam na última execução (a partir de run_results.json
    ou de uma tabela de auditoria populada pelo job do dbt)."""
    raise NotImplementedError("Depende da tabela de auditoria de testes dbt (etapa 3).")


def query_price_window(symbol: str, start, end) -> list[dict]:
    """Retorna os preços agregados de um ativo em uma janela de tempo, para o
    agente correlacionar com o horário de uma falha de teste."""
    query = f"""
        select window_start, avg_price, price_volatility, price_range_pct
        from `{GCP_PROJECT}.{GOLD_DATASET}.fct_price_ticks`
        where symbol = @symbol and window_start between @start and @end
        order by window_start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
        ]
    )
    result = BQ_CLIENT.query(query, job_config=job_config)
    return [dict(row) for row in result]
