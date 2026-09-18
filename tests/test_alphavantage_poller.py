from unittest.mock import Mock, patch

from ingestion.alphavantage_poller import fetch_quote


def _mock_response(json_body):
    response = Mock()
    response.raise_for_status = Mock()
    response.json.return_value = json_body
    return response


@patch("ingestion.alphavantage_poller.requests.get")
def test_fetch_quote_parses_global_quote(mock_get):
    mock_get.return_value = _mock_response(
        {
            "Global Quote": {
                "01. symbol": "IBM",
                "05. price": "243.29",
                "06. volume": "4919572",
                "07. latest trading day": "2026-09-14",
                "08. previous close": "240.00",
                "10. change percent": "1.37%",
            }
        }
    )

    quote = fetch_quote("IBM")

    assert quote == {
        "symbol": "IBM",
        "price": "243.29",
        "volume": "4919572",
        "latest_trading_day": "2026-09-14",
        "previous_close": "240.00",
        "change_percent": "1.37%",
    }


@patch("ingestion.alphavantage_poller.requests.get")
def test_fetch_quote_returns_none_on_throttling_note(mock_get):
    mock_get.return_value = _mock_response(
        {"Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute."}
    )

    assert fetch_quote("IBM") is None


@patch("ingestion.alphavantage_poller.requests.get")
def test_fetch_quote_returns_none_when_quote_missing(mock_get):
    mock_get.return_value = _mock_response({})

    assert fetch_quote("IBM") is None
