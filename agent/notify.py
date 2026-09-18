"""Envio do diagnóstico do agente para um canal externo (Slack/Discord via webhook).

Ainda não implementado — depende de agent.py estar gerando diagnósticos reais.
"""
import os

import requests

WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "")


def send_report(summary: str) -> None:
    if not WEBHOOK_URL:
        raise RuntimeError("NOTIFY_WEBHOOK_URL não configurada no .env.")

    response = requests.post(WEBHOOK_URL, json={"text": summary}, timeout=10)
    response.raise_for_status()
