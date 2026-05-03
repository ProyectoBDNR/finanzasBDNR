"""
infra/smoke_test.py
--------------------
Verifica que el proyecto está correctamente instalado.
No requiere Cassandra ni Spark activos.

Uso:
    python infra/smoke_test.py
    # → ALL OK  (o lista de errores)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

checks = []


def check(name: str, fn):
    try:
        fn()
        checks.append((name, "OK"))
        print(f"  ✓  {name}")
    except Exception as exc:
        checks.append((name, f"FAIL: {exc}"))
        print(f"  ✗  {name}  →  {exc}")


# ── Imports ────────────────────────────────────────────────────────────────
check("import consumer.models",        lambda: __import__("consumer.models"))
check("import consumer.binance_ws",    lambda: __import__("consumer.binance_ws"))
check("import consumer.logger",        lambda: __import__("consumer.logger"))
check("import storage.session",        lambda: __import__("storage.session"))
check("import storage.cassandra_writer", lambda: __import__("storage.cassandra_writer"))
check("import storage.schema_manager", lambda: __import__("storage.schema_manager"))
check("import enrichment.coingecko",   lambda: __import__("enrichment.coingecko"))
check("import processing.cleaner",     lambda: __import__("processing.cleaner"))
check("import processing.aggregator",  lambda: __import__("processing.aggregator"))
check("import processing.job",         lambda: __import__("processing.job"))
check("import feature_engine.features", lambda: __import__("feature_engine.features"))
check("import analytics.queries",      lambda: __import__("analytics.queries"))
check("import analytics.cql_queries",  lambda: __import__("analytics.cql_queries"))

# ── Parser ─────────────────────────────────────────────────────────────────
import json
from consumer.binance_ws import parse_event
from consumer.models import AggTrade, BookTicker

AGG_PAYLOAD = json.dumps({
    "stream": "btcusdt@aggTrade",
    "data": {"e": "aggTrade", "E": 1718000000123, "s": "BTCUSDT",
             "a": 1, "p": "67000.00", "q": "0.1", "T": 1718000000100, "m": False},
})
TICKER_PAYLOAD = json.dumps({
    "stream": "btcusdt@bookTicker",
    "data": {"e": "bookTicker", "E": 1718000001000, "s": "BTCUSDT",
             "b": "66999.00", "B": "1.0", "a": "67001.00", "A": "0.5"},
})

check("parse aggTrade",    lambda: (lambda e: None if not isinstance(e, AggTrade)    else None)(parse_event(AGG_PAYLOAD)))
check("parse bookTicker",  lambda: (lambda e: None if not isinstance(e, BookTicker)  else None)(parse_event(TICKER_PAYLOAD)))
check("trace_id unique",   lambda: (lambda a, b: (_ for _ in ()).throw(AssertionError("not unique")) if a.trace_id == b.trace_id else None)(AggTrade("BTC",1,1.0,1.0,1,1,False), AggTrade("BTC",2,1.0,1.0,1,1,False)))

# ── Storage helpers ────────────────────────────────────────────────────────
from storage.cassandra_writer import _ms_to_date
check("ms_to_date 2024-06-10", lambda: (None if _ms_to_date(1718000000000) == "2024-06-10" else (_ for _ in ()).throw(AssertionError(_ms_to_date(1718000000000)))))

# ── Schema file ────────────────────────────────────────────────────────────
from pathlib import Path
check("schemas/cassandra.cql exists", lambda: (None if Path("schemas/cassandra.cql").exists() else (_ for _ in ()).throw(FileNotFoundError("schemas/cassandra.cql"))))

# ── CoinGecko mock ─────────────────────────────────────────────────────────
from enrichment.coingecko import get_all_metadata
check("coingecko mock 3 symbols", lambda: (None if len(get_all_metadata(use_mock=True)) == 3 else (_ for _ in ()).throw(AssertionError("expected 3"))))

# ── Resultado ──────────────────────────────────────────────────────────────
failed = [c for c in checks if not c[1].startswith("OK")]
print(f"\n{'─'*45}")
print(f"  {len(checks) - len(failed)}/{len(checks)} checks passed")
print(f"  {'ALL OK ✓' if not failed else str(len(failed)) + ' FAILED ✗'}")
sys.exit(1 if failed else 0)
