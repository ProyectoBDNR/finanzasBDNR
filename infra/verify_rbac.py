"""
infra/verify_rbac.py
---------------------
Verifica que el modelo RBAC de Cassandra está correctamente configurado.

Prueba que:
  - cf_writer puede INSERT en raw_trades y raw_book_tickers
  - cf_writer NO puede SELECT (Unauthorized esperado)
  - cf_analyst puede SELECT en todas las tablas
  - cf_analyst NO puede INSERT en tablas raw (Unauthorized esperado)
  - cf_admin puede hacer cualquier operación

Uso:
    python infra/verify_rbac.py

Requiere:
    CASSANDRA_WRITER_USER / CASSANDRA_WRITER_PASSWORD
    CASSANDRA_ANALYST_USER / CASSANDRA_ANALYST_PASSWORD
    CASSANDRA_ADMIN_USER / CASSANDRA_ADMIN_PASSWORD
    o bien: CASSANDRA_SUPER_USER / CASSANDRA_SUPER_PASSWORD
"""

from __future__ import annotations

import os
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")


def _connect(user: str, pwd: str):
    auth = PlainTextAuthProvider(username=user, password=pwd)
    cluster = Cluster(
        contact_points=CASSANDRA_HOSTS,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
    )
    return cluster, cluster.connect("cryptoflow")


def _test(label: str, fn, expect_fail: bool = False) -> bool:
    try:
        fn()
        if expect_fail:
            print(f"  ✗ FALLO  {label} — debería haber fallado (Unauthorized)")
            return False
        print(f"  ✓ OK     {label}")
        return True
    except Exception as e:
        if expect_fail and ("Unauthorized" in str(e) or "unauthorized" in str(e).lower()
                            or "not authorized" in str(e).lower()):
            print(f"  ✓ OK     {label} — Unauthorized (esperado)")
            return True
        print(f"  ✗ FALLO  {label} — {e}")
        return False


def verify_writer():
    print("\n── cf_writer ──────────────────────────────────────────────")
    user = os.getenv("CASSANDRA_WRITER_USER", "cf_writer")
    pwd  = os.getenv("CASSANDRA_WRITER_PASSWORD", "writer_pwd_BDNR")
    try:
        cluster, session = _connect(user, pwd)
    except Exception as e:
        print(f"  ✗ No se pudo conectar como {user}: {e}")
        return 0, 1

    results = []
    # Debe poder hacer INSERT
    results.append(_test(
        "INSERT en raw_trades",
        lambda: session.execute(
            "INSERT INTO cryptoflow.raw_trades "
            "(symbol, date, trade_time, agg_trade_id, price, quantity, "
            "is_buyer_maker, trace_id, event_time) "
            "VALUES ('TESTUSDT','1970-01-01',0,0,1.0,1.0,false,"
            "00000000-0000-0000-0000-000000000000,0)"
        )
    ))
    results.append(_test(
        "INSERT en raw_book_tickers",
        lambda: session.execute(
            "INSERT INTO cryptoflow.raw_book_tickers "
            "(symbol, date, event_time, trace_id, best_bid_price, "
            "best_ask_price, best_bid_qty, best_ask_qty, spread) "
            "VALUES ('TESTUSDT','1970-01-01',0,"
            "00000000-0000-0000-0000-000000000000,1.0,1.0,1.0,1.0,0.0)"
        )
    ))
    # NO debe poder hacer SELECT
    results.append(_test(
        "SELECT en raw_trades (debe fallar)",
        lambda: session.execute("SELECT * FROM cryptoflow.raw_trades LIMIT 1"),
        expect_fail=True
    ))
    results.append(_test(
        "INSERT en ohlcv_1m (debe fallar)",
        lambda: session.execute(
            "INSERT INTO cryptoflow.ohlcv_1m (symbol, window_label, window_start) "
            "VALUES ('X','X',0)"
        ),
        expect_fail=True
    ))
    cluster.shutdown()
    ok = sum(results)
    print(f"  Resultado: {ok}/{len(results)} pruebas pasaron")
    return ok, len(results)


def verify_analyst():
    print("\n── cf_analyst ─────────────────────────────────────────────")
    user = os.getenv("CASSANDRA_ANALYST_USER", "cf_analyst")
    pwd  = os.getenv("CASSANDRA_ANALYST_PASSWORD", "analyst_pwd_BDNR")
    try:
        cluster, session = _connect(user, pwd)
    except Exception as e:
        print(f"  ✗ No se pudo conectar como {user}: {e}")
        return 0, 1

    results = []
    # Debe poder SELECT en todas las tablas
    for table in ["raw_trades", "raw_book_tickers", "ohlcv_1m", "ohlcv_5m",
                  "ohlcv_1h", "features_by_window"]:
        results.append(_test(
            f"SELECT en {table}",
            lambda t=table: session.execute(f"SELECT * FROM cryptoflow.{t} LIMIT 1")
        ))
    # NO debe poder INSERT en raw
    results.append(_test(
        "INSERT en raw_trades (debe fallar)",
        lambda: session.execute(
            "INSERT INTO cryptoflow.raw_trades "
            "(symbol, date, trade_time, agg_trade_id) "
            "VALUES ('X','X',0,0)"
        ),
        expect_fail=True
    ))
    cluster.shutdown()
    ok = sum(results)
    print(f"  Resultado: {ok}/{len(results)} pruebas pasaron")
    return ok, len(results)


def verify_admin():
    print("\n── cf_admin ───────────────────────────────────────────────")
    user = os.getenv("CASSANDRA_ADMIN_USER", "cf_admin")
    pwd  = os.getenv("CASSANDRA_ADMIN_PASSWORD", "admin_pwd_BDNR")
    try:
        cluster, session = _connect(user, pwd)
    except Exception as e:
        print(f"  ✗ No se pudo conectar como {user}: {e}")
        return 0, 1

    results = []
    results.append(_test(
        "SELECT en raw_trades",
        lambda: session.execute("SELECT * FROM cryptoflow.raw_trades LIMIT 1")
    ))
    results.append(_test(
        "SELECT en ohlcv_1h",
        lambda: session.execute("SELECT * FROM cryptoflow.ohlcv_1h LIMIT 1")
    ))
    results.append(_test(
        "LIST ROLES (privilegio administrativo)",
        lambda: session.execute("LIST ROLES")
    ))
    cluster.shutdown()
    ok = sum(results)
    print(f"  Resultado: {ok}/{len(results)} pruebas pasaron")
    return ok, len(results)


if __name__ == "__main__":
    print("═"*60)
    print("  CryptoFlow — Verificación RBAC")
    print("═"*60)

    total_ok, total_tests = 0, 0
    for fn in [verify_writer, verify_analyst, verify_admin]:
        ok, tests = fn()
        total_ok    += ok
        total_tests += tests

    print(f"\n{'═'*60}")
    print(f"  Total: {total_ok}/{total_tests} pruebas pasaron")
    if total_ok == total_tests:
        print("  ✓ RBAC configurado correctamente")
    else:
        print("  ✗ Hay pruebas fallidas — revisar permisos")
    print("═"*60)
    sys.exit(0 if total_ok == total_tests else 1)
