import json

from ingestion.common.writer import BronzeWriter


def test_write_creates_partitioned_jsonl_file(tmp_path):
    writer = BronzeWriter(tmp_path, source="binance")

    assert writer.write({"symbol": "BTCUSDT"}) is True

    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
    # <base_dir>/<source>/<YYYY-MM-DD>/<HH>.jsonl
    assert files[0].parent.parent.name == "binance"
    assert files[0].suffix == ".jsonl"


def test_write_record_shape(tmp_path):
    writer = BronzeWriter(tmp_path, source="binance")
    writer.write({"symbol": "BTCUSDT"})

    line = next(tmp_path.rglob("*.jsonl")).read_text(encoding="utf-8").strip()
    record = json.loads(line)

    assert record["source"] == "binance"
    assert record["payload"] == {"symbol": "BTCUSDT"}
    assert "ingested_at" in record


def test_write_deduplicates_by_key(tmp_path):
    writer = BronzeWriter(tmp_path, source="binance")

    assert writer.write({"symbol": "BTCUSDT"}, dedupe_key="BTCUSDT:1") is True
    assert writer.write({"symbol": "BTCUSDT"}, dedupe_key="BTCUSDT:1") is False

    lines = next(tmp_path.rglob("*.jsonl")).read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1


def test_write_without_dedupe_key_never_skips(tmp_path):
    writer = BronzeWriter(tmp_path, source="alphavantage")

    writer.write({"symbol": "IBM"})
    writer.write({"symbol": "IBM"})

    lines = next(tmp_path.rglob("*.jsonl")).read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
