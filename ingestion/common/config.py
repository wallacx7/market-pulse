"""Configuração central da camada de ingestão."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

# Binance (streaming, sem necessidade de API key para dados públicos de mercado)
BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443/stream"
BINANCE_SYMBOLS = os.environ.get("BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")

# Alpha Vantage (REST polling)
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
ALPHA_VANTAGE_SYMBOLS = os.environ.get("ALPHA_VANTAGE_SYMBOLS", "IBM,AAPL,MSFT").split(",")
# Free tier: 5 requisições/min, 25/dia. Com N símbolos, cada um é consultado a cada
# ALPHA_VANTAGE_POLL_INTERVAL_SECONDS, respeitando o limite.
ALPHA_VANTAGE_POLL_INTERVAL_SECONDS = int(os.environ.get("ALPHA_VANTAGE_POLL_INTERVAL_SECONDS", "60"))
