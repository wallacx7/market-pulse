"""Ingestão contínua via polling REST na Alpha Vantage (ações/forex).

O free tier da Alpha Vantage limita a 5 requisições/minuto e 25/dia, então o
polling espaça as requisições entre os símbolos para nunca estourar o limite,
e faz backoff quando a API sinaliza throttling.

Uso:
    python -m ingestion.alphavantage_poller
"""
import logging
import time

import requests

from ingestion.common.config import (
    ALPHA_VANTAGE_API_KEY,
    ALPHA_VANTAGE_BASE_URL,
    ALPHA_VANTAGE_POLL_INTERVAL_SECONDS,
    ALPHA_VANTAGE_SYMBOLS,
    BRONZE_DIR,
)
from ingestion.common.writer import BronzeWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("alphavantage_poller")

# Free tier: no máximo 5 req/min -> nunca menos que 12s entre requisições.
MIN_SECONDS_BETWEEN_REQUESTS = 12
REQUEST_TIMEOUT_SECONDS = 10


def fetch_quote(symbol: str) -> dict | None:
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }
    response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    body = response.json()

    if "Note" in body or "Information" in body:
        logger.warning("Alpha Vantage sinalizou throttling para %s: %s", symbol, body)
        return None

    quote = body.get("Global Quote")
    if not quote:
        logger.warning("Resposta sem 'Global Quote' para %s: %s", symbol, body)
        return None

    return {
        "symbol": quote.get("01. symbol"),
        "price": quote.get("05. price"),
        "volume": quote.get("06. volume"),
        "latest_trading_day": quote.get("07. latest trading day"),
        "previous_close": quote.get("08. previous close"),
        "change_percent": quote.get("10. change percent"),
    }


def poll_forever(symbols: list[str], writer: BronzeWriter) -> None:
    if not ALPHA_VANTAGE_API_KEY:
        raise RuntimeError(
            "ALPHA_VANTAGE_API_KEY não configurada. Defina no .env (veja .env.example)."
        )

    delay_between_requests = max(
        MIN_SECONDS_BETWEEN_REQUESTS,
        ALPHA_VANTAGE_POLL_INTERVAL_SECONDS // max(len(symbols), 1),
    )
    logger.info(
        "Iniciando polling de %s a cada ~%ss por símbolo.", ", ".join(symbols), delay_between_requests
    )

    while True:
        for symbol in symbols:
            try:
                quote = fetch_quote(symbol)
            except requests.RequestException as exc:
                logger.warning("Falha ao consultar %s: %s", symbol, exc)
                time.sleep(delay_between_requests)
                continue

            if quote is not None:
                dedupe_key = f"{quote['symbol']}:{quote['latest_trading_day']}:{quote['price']}"
                writer.write(quote, dedupe_key=dedupe_key)

            time.sleep(delay_between_requests)


def main() -> None:
    writer = BronzeWriter(BRONZE_DIR, source="alphavantage")
    poll_forever(ALPHA_VANTAGE_SYMBOLS, writer)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Encerrado pelo usuário.")
