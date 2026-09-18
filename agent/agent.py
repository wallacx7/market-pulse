"""Agente de diagnóstico de anomalias de mercado.

Fluxo pretendido (a implementar na etapa 3, depois de dbt/ estar rodando):

1. Ler os testes dbt que falharam na última execução (`tools.query_failed_tests`).
2. Para cada falha, consultar o histórico de preço do ativo no período
   (`tools.query_price_window`) para correlacionar com picos de volatilidade.
3. Montar um diagnóstico em linguagem natural explicando o que aconteceu.
4. Gravar o resultado em reports/ (ver report_writer) e opcionalmente notificar
   (ver notify.py).

O provedor do modelo usado para gerar o diagnóstico ainda não foi decidido —
ver TODO abaixo antes de implementar.
"""


def diagnose_failures() -> list[dict]:
    """Ponto de entrada principal do agente. Ainda não implementado."""
    raise NotImplementedError(
        "Implementar depois que dbt/ estiver gerando testes reais para investigar."
    )


if __name__ == "__main__":
    diagnose_failures()
