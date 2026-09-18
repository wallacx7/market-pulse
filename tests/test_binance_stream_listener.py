import json

from ingestion.binance_stream_listener import build_stream_url, parse_trade_event


def test_build_stream_url_joins_symbols_lowercase():
    url = build_stream_url(["BTCUSDT", " ethusdt "])

    assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"


def test_build_stream_url_skips_blank_symbols():
    url = build_stream_url(["BTCUSDT", "", "  "])

    assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@trade"


def test_parse_trade_event_extracts_payload_and_dedupe_key():
    message = json.dumps(
        {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT",
                "t": 12345,
                "p": "65000.10",
                "q": "0.01",
                "T": 1700000000000,
                "m": True,
            },
        }
    )

    result = parse_trade_event(message)

    assert result is not None
    payload, dedupe_key = result
    assert payload == {
        "symbol": "BTCUSDT",
        "trade_id": 12345,
        "price": "65000.10",
        "quantity": "0.01",
        "trade_time_ms": 1700000000000,
        "is_buyer_maker": True,
    }
    assert dedupe_key == "BTCUSDT:12345"


def test_parse_trade_event_ignores_non_trade_events():
    message = json.dumps({"data": {"e": "depthUpdate"}})

    assert parse_trade_event(message) is None


def test_parse_trade_event_ignores_envelopes_without_data():
    message = json.dumps({"result": None, "id": 1})

    assert parse_trade_event(message) is None
