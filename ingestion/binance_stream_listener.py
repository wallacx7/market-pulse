"""Ingestão contínua via WebSocket da Binance (dados públicos, sem API key).

Conecta no stream combinado de trades para os símbolos configurados e grava
cada evento bruto na camada Bronze. Reconecta automaticamente com backoff
exponencial em caso de queda de conexão.

Uso:
    python -m ingestion.binance_stream_listener
"""
import asyncio
import json
import logging
import signal

import websockets
from websockets.exceptions import ConnectionClosed

from ingestion.common.config import BINANCE_SYMBOLS, BINANCE_WS_BASE_URL, BRONZE_DIR
from ingestion.common.writer import BronzeWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("binance_stream_listener")

MAX_BACKOFF_SECONDS = 60


def build_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.strip().lower()}@trade" for symbol in symbols if symbol.strip())
    return f"{BINANCE_WS_BASE_URL}?streams={streams}"


def parse_trade_event(message: str) -> tuple[dict, str] | None:
    """Extrai o payload do trade e uma chave de deduplicação (símbolo + trade id)."""
    envelope = json.loads(message)
    data = envelope.get("data")
    if not data or data.get("e") != "trade":
        return None

    payload = {
        "symbol": data["s"],
        "trade_id": data["t"],
        "price": data["p"],
        "quantity": data["q"],
        "trade_time_ms": data["T"],
        "is_buyer_maker": data["m"],
    }
    dedupe_key = f"{payload['symbol']}:{payload['trade_id']}"
    return payload, dedupe_key


async def listen(symbols: list[str], writer: BronzeWriter, stop_event: asyncio.Event) -> None:
    url = build_stream_url(symbols)
    backoff = 1

    while not stop_event.is_set():
        try:
            logger.info("Conectando em %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                backoff = 1
                logger.info("Conectado. Recebendo trades de: %s", ", ".join(symbols))

                while not stop_event.is_set():
                    message = await ws.recv()
                    parsed = parse_trade_event(message)
                    if parsed is None:
                        continue
                    payload, dedupe_key = parsed
                    writer.write(payload, dedupe_key=dedupe_key)

        except (ConnectionClosed, OSError) as exc:
            if stop_event.is_set():
                break
            logger.warning("Conexão perdida (%s). Reconectando em %ss...", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        except json.JSONDecodeError:
            logger.warning("Mensagem malformada recebida, ignorando.")


async def main() -> None:
    writer = BronzeWriter(BRONZE_DIR, source="binance")
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows não suporta add_signal_handler para todos os sinais.
            pass

    await listen(BINANCE_SYMBOLS, writer, stop_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Encerrado pelo usuário.")
