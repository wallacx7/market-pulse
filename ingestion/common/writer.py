"""Escrita da camada Bronze: JSON bruto, particionado por data, com deduplicação."""
import json
import threading
from datetime import datetime, timezone
from pathlib import Path


class BronzeWriter:
    """Grava eventos como JSON Lines em arquivos particionados por dia.

    Mantém um conjunto de chaves já vistas nesta execução para evitar gravar
    o mesmo evento duas vezes (ex: reconexão de WebSocket reenviando o último
    trade). A deduplicação é best-effort e por processo, não persistida entre
    execuções — reprocessamento downstream ainda deve tratar dado bruto como
    "at-least-once".
    """

    def __init__(self, base_dir: Path, source: str):
        self.base_dir = base_dir
        self.source = source
        self._lock = threading.Lock()
        self._seen_keys: set[str] = set()

    def _partition_path(self, when: datetime) -> Path:
        day_dir = self.base_dir / self.source / when.strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir / f"{when.strftime('%H')}.jsonl"

    def write(self, payload: dict, dedupe_key: str | None = None) -> bool:
        """Grava um evento. Retorna False se foi descartado por ser duplicado."""
        now = datetime.now(timezone.utc)

        with self._lock:
            if dedupe_key is not None:
                if dedupe_key in self._seen_keys:
                    return False
                self._seen_keys.add(dedupe_key)

            record = {
                "ingested_at": now.isoformat(),
                "source": self.source,
                "payload": payload,
            }
            path = self._partition_path(now)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return True
