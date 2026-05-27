"""
analytics/trace_lifecycle.py
-----------------------------
Demo de trazabilidad end-to-end: dado un trace_id (UUID v4) generado por
``consumer/binance_ws.py``, recorre las 4 etapas del ciclo de vida del dato
y muestra cómo el evento crudo se transformó hasta convertirse en información
estratégica.

Etapas:
  1) Evento crudo                → cryptoflow.raw_trades
  2) Dato limpio + agregado      → cryptoflow.ohlcv_1m
  3) Feature cuantitativa        → cryptoflow.features_by_window
  4) Información estratégica     → Q1 (régimen de volatilidad LOW/MED/HIGH)

Uso:
    python -m analytics.trace_lifecycle --symbol BTCUSDT --date 2026-05-26
    python -m analytics.trace_lifecycle --symbol BTCUSDT --date 2026-05-26 \
        --trace-id a1b2c3d4-1234-5678-9abc-def012345678

Decisión de diseño:
  Se usa el driver nativo `cassandra-driver` (no Spark) porque la operación es
  un lookup puntual sobre 3 particiones bien conocidas. Spark añadiría minutos
  de cold-start para un trabajo que en CQL toma decenas de ms.

Restricción de Cassandra:
  raw_trades.PRIMARY KEY = ((symbol, date), trade_time, agg_trade_id).
  trace_id NO es partition key. Para buscar por trace_id necesitamos al menos
  (symbol, date) para acotar la búsqueda a una sola partición; dentro de esa
  partición sí podemos filtrar por trace_id usando ALLOW FILTERING sin pagar
  el costo de un scan distribuido.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

# Permite ejecución como `python -m analytics.trace_lifecycle`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.session import get_session, close as close_session


SEP_MAJOR = "═" * 78
SEP_MINOR = "─" * 78


def _print_section(num: int, title: str, table: str) -> None:
    print()
    print(SEP_MAJOR)
    print(f"  ETAPA {num} · {title}")
    print(f"  Fuente: cryptoflow.{table}")
    print(SEP_MAJOR)


def _format_kv(label: str, value, width: int = 22) -> str:
    return f"  {label:<{width}} {value}"


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _window_start_for_trade(trade_time_ms: int) -> datetime:
    """Floor del trade_time al inicio de la ventana de 1 minuto (UTC)."""
    floored_ms = (trade_time_ms // 60_000) * 60_000
    return datetime.fromtimestamp(floored_ms / 1000, tz=timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Etapa 1 · raw_trades
# ─────────────────────────────────────────────────────────────────────────────

def fetch_raw_trade(session, symbol: str, date: str,
                    trace_id: Optional[str]) -> Optional[dict]:
    """
    Localiza un trade dentro de la partición (symbol, date).

    Si ``trace_id`` es None se elige uno al azar de la partición (útil para
    demos rápidos). Si se provee, se busca exactamente esa fila.
    """
    if trace_id is not None:
        # Filtro dentro de una sola partición: el ALLOW FILTERING es barato
        # porque la partición ya está acotada por (symbol, date).
        query = """
            SELECT symbol, date, trade_time, event_time, agg_trade_id,
                   price, quantity, is_buyer_maker, trace_id, ingestion_ts
            FROM cryptoflow.raw_trades
            WHERE symbol = %(symbol)s
              AND date   = %(date)s
              AND trace_id = %(trace_id)s
            ALLOW FILTERING;
        """
        rows = list(session.execute(query, {
            "symbol":   symbol,
            "date":     date,
            "trace_id": uuid.UUID(trace_id),
        }))
        return dict(rows[0]._asdict()) if rows else None

    # Muestreo: traemos los últimos 200 trades de la partición y elegimos uno.
    query = """
        SELECT symbol, date, trade_time, event_time, agg_trade_id,
               price, quantity, is_buyer_maker, trace_id, ingestion_ts
        FROM cryptoflow.raw_trades
        WHERE symbol = %(symbol)s
          AND date   = %(date)s
        LIMIT 200;
    """
    rows = list(session.execute(query, {"symbol": symbol, "date": date}))
    if not rows:
        return None
    return dict(random.choice(rows)._asdict())


def print_stage_1(trade: dict) -> None:
    _print_section(1, "Evento crudo (aggTrade de Binance)", "raw_trades")
    print(_format_kv("symbol",          trade["symbol"]))
    print(_format_kv("date (partition)", trade["date"]))
    print(_format_kv("agg_trade_id",    trade["agg_trade_id"]))
    print(_format_kv("price",           f"{trade['price']:.4f}"))
    print(_format_kv("quantity",        f"{trade['quantity']:.6f}"))
    print(_format_kv("is_buyer_maker",  trade["is_buyer_maker"]))
    print(_format_kv("trade_time (ms)", trade["trade_time"]))
    print(_format_kv("  → trade_time UTC", _ms_to_iso(trade["trade_time"])))
    print(_format_kv("event_time (ms)", trade["event_time"]))
    print(_format_kv("  → event_time UTC", _ms_to_iso(trade["event_time"])))
    print(_format_kv("ingestion_ts",    trade["ingestion_ts"]))
    print(_format_kv("trace_id",        trade["trace_id"]))
    binance_lat = trade["event_time"] - trade["trade_time"]
    print(SEP_MINOR)
    print(f"  Latencia interna Binance (event_time - trade_time): {binance_lat} ms")


# ─────────────────────────────────────────────────────────────────────────────
# Etapa 2 · ohlcv_1m
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ohlcv_1m(session, symbol: str, window_start: datetime) -> Optional[dict]:
    query = """
        SELECT symbol, window_label, window_start, window_end,
               open, high, low, close, volume, trade_count,
               buy_volume, sell_volume
        FROM cryptoflow.ohlcv_1m
        WHERE symbol = %(symbol)s
          AND window_label = '1m'
          AND window_start = %(window_start)s;
    """
    rows = list(session.execute(query, {
        "symbol":       symbol,
        "window_start": window_start,
    }))
    return dict(rows[0]._asdict()) if rows else None


def print_stage_2(ohlcv: Optional[dict], window_start: datetime) -> None:
    _print_section(2, "Dato limpio y agregado (OHLCV 1 minuto)", "ohlcv_1m")
    if ohlcv is None:
        print(f"  ⚠ No existe ventana 1m con window_start={window_start.isoformat()}")
        print("    Posibles causas: la ventana aún no fue agregada por Spark,")
        print("    o el trade cae fuera del rango de datos procesados.")
        return
    print(_format_kv("symbol",        ohlcv["symbol"]))
    print(_format_kv("window_label",  ohlcv["window_label"]))
    print(_format_kv("window_start",  ohlcv["window_start"]))
    print(_format_kv("window_end",    ohlcv["window_end"]))
    print(SEP_MINOR)
    print(_format_kv("open",          f"{ohlcv['open']:.4f}"))
    print(_format_kv("high",          f"{ohlcv['high']:.4f}"))
    print(_format_kv("low",           f"{ohlcv['low']:.4f}"))
    print(_format_kv("close",         f"{ohlcv['close']:.4f}"))
    print(_format_kv("volume",        f"{ohlcv['volume']:.6f}"))
    print(_format_kv("trade_count",   ohlcv["trade_count"]))
    print(_format_kv("buy_volume",    f"{ohlcv['buy_volume']:.6f}"))
    print(_format_kv("sell_volume",   f"{ohlcv['sell_volume']:.6f}"))


# ─────────────────────────────────────────────────────────────────────────────
# Etapa 3 · features_by_window
# ─────────────────────────────────────────────────────────────────────────────

def fetch_feature_row(session, symbol: str, window_start: datetime) -> Optional[dict]:
    query = """
        SELECT symbol, window_label, window_start, window_end,
               close, vwap, log_return,
               rolling_volatility, realized_volatility,
               momentum, momentum_pct, buy_sell_ratio,
               spread_mean, spread_mean_pct, mid_price_mean, obi
        FROM cryptoflow.features_by_window
        WHERE symbol = %(symbol)s
          AND window_label = '1m'
          AND window_start = %(window_start)s;
    """
    rows = list(session.execute(query, {
        "symbol":       symbol,
        "window_start": window_start,
    }))
    return dict(rows[0]._asdict()) if rows else None


def _fmt(v, fmt: str = ".6f") -> str:
    if v is None:
        return "NULL"
    try:
        return format(v, fmt)
    except (TypeError, ValueError):
        return str(v)


def print_stage_3(feat: Optional[dict]) -> None:
    _print_section(3, "Feature cuantitativa (indicadores técnicos)",
                   "features_by_window")
    if feat is None:
        print("  ⚠ No existe fila de features para esta ventana.")
        print("    El Feature Engine aún no procesó la ventana, o se omitió.")
        return
    print(_format_kv("symbol",              feat["symbol"]))
    print(_format_kv("window_label",        feat["window_label"]))
    print(_format_kv("window_start",        feat["window_start"]))
    print(SEP_MINOR)
    print(_format_kv("vwap",                _fmt(feat["vwap"])))
    print(_format_kv("log_return",          _fmt(feat["log_return"], ".8f")))
    print(_format_kv("rolling_volatility",  _fmt(feat["rolling_volatility"], ".8f")))
    print(_format_kv("realized_volatility", _fmt(feat["realized_volatility"], ".8f")))
    print(_format_kv("momentum",            _fmt(feat["momentum"])))
    print(_format_kv("momentum_pct",        _fmt(feat["momentum_pct"], ".6f")))
    print(_format_kv("buy_sell_ratio",      _fmt(feat["buy_sell_ratio"])))
    print(_format_kv("spread_mean",         _fmt(feat["spread_mean"])))
    print(_format_kv("spread_mean_pct",     _fmt(feat["spread_mean_pct"])))
    print(_format_kv("mid_price_mean",      _fmt(feat["mid_price_mean"], ".4f")))
    print(_format_kv("obi",                 _fmt(feat["obi"])))


# ─────────────────────────────────────────────────────────────────────────────
# Etapa 4 · Q1 — Régimen de volatilidad LOW/MED/HIGH
# ─────────────────────────────────────────────────────────────────────────────

def classify_regime(session, symbol: str, target_vol: Optional[float]
                    ) -> tuple[Optional[str], Optional[float], Optional[float], int]:
    """
    Reproduce la lógica de Q1 (analytics/queries.py:q1_volatility_regime)
    pero en CQL puro:
      1) Trae todos los rolling_volatility del símbolo en ventanas de 1m.
      2) Calcula percentiles p33 y p66 en cliente.
      3) Clasifica target_vol según los umbrales.

    Returns: (regime, p33, p66, sample_size)
    """
    if target_vol is None:
        return None, None, None, 0

    query = """
        SELECT rolling_volatility
        FROM cryptoflow.features_by_window
        WHERE symbol = %(symbol)s
          AND window_label = '1m';
    """
    rows = session.execute(query, {"symbol": symbol})
    vols = sorted(
        r.rolling_volatility for r in rows
        if r.rolling_volatility is not None
    )
    n = len(vols)
    if n < 3:
        return "UNKNOWN", None, None, n

    p33 = vols[int(0.33 * (n - 1))]
    p66 = vols[int(0.66 * (n - 1))]

    if target_vol <= p33:
        regime = "LOW"
    elif target_vol <= p66:
        regime = "MED"
    else:
        regime = "HIGH"
    return regime, p33, p66, n


def print_stage_4(symbol: str, target_vol: Optional[float],
                  regime: Optional[str], p33: Optional[float],
                  p66: Optional[float], sample_size: int) -> None:
    _print_section(4, "Información estratégica (Q1 · régimen de volatilidad)",
                   "queries.q1_volatility_regime")
    print(_format_kv("símbolo",                symbol))
    print(_format_kv("sample size (ventanas)", sample_size))
    if regime is None:
        print("  ⚠ La etapa 3 no expuso rolling_volatility — no se puede clasificar.")
        return
    if regime == "UNKNOWN":
        print("  ⚠ Insuficientes ventanas con rolling_volatility (<3) para calcular")
        print("    percentiles. Esperar a que el Feature Engine procese más datos.")
        return
    print(_format_kv("umbral p33",             _fmt(p33, ".8f")))
    print(_format_kv("umbral p66",             _fmt(p66, ".8f")))
    print(_format_kv("rolling_volatility ahora", _fmt(target_vol, ".8f")))
    print(SEP_MINOR)
    flag = {"LOW": "🟢", "MED": "🟡", "HIGH": "🔴"}.get(regime, "·")
    print(f"  Régimen clasificado: {flag} {regime}")
    if regime == "LOW":
        meaning = ("Mercado tranquilo — spreads estrechos, posiciones de mayor "
                   "tamaño son seguras.")
    elif regime == "MED":
        meaning = ("Volatilidad típica — operación normal, sin alertas de riesgo "
                   "elevado.")
    else:  # HIGH
        meaning = ("Volatilidad elevada — recortar tamaño de posición, "
                   "ampliar stops, vigilar liquidez.")
    print(f"  Lectura accionable: {meaning}")


# ─────────────────────────────────────────────────────────────────────────────
# Orquestación
# ─────────────────────────────────────────────────────────────────────────────

def run(symbol: str, date: str, trace_id: Optional[str]) -> int:
    session = get_session()

    print()
    print(SEP_MAJOR)
    print("  CICLO DE VIDA DEL DATO · evento crudo → información estratégica")
    print(SEP_MAJOR)
    print(_format_kv("symbol",   symbol))
    print(_format_kv("date",     date))
    print(_format_kv("trace_id", trace_id if trace_id else "(aleatorio)"))

    # Etapa 1
    trade = fetch_raw_trade(session, symbol, date, trace_id)
    if trade is None:
        if trace_id is not None:
            print()
            print(f"❌ trace_id {trace_id} no encontrado en raw_trades")
            print(f"   para partición (symbol={symbol}, date={date}).")
            print("   Verifica el UUID, el símbolo y la fecha, o que el evento")
            print("   no haya expirado por TTL (7 días).")
        else:
            print()
            print(f"❌ La partición (symbol={symbol}, date={date}) está vacía.")
            print("   ¿El consumer está corriendo? ¿La fecha es correcta?")
        return 2

    print_stage_1(trade)

    # Etapa 2
    window_start = _window_start_for_trade(trade["trade_time"])
    ohlcv = fetch_ohlcv_1m(session, symbol, window_start)
    print_stage_2(ohlcv, window_start)

    # Etapa 3
    feat = fetch_feature_row(session, symbol, window_start)
    print_stage_3(feat)

    # Etapa 4
    target_vol = feat["rolling_volatility"] if feat else None
    regime, p33, p66, n = classify_regime(session, symbol, target_vol)
    print_stage_4(symbol, target_vol, regime, p33, p66, n)

    print()
    print(SEP_MAJOR)
    print(f"  ✓ Trazabilidad completa para trace_id = {trade['trace_id']}")
    print(SEP_MAJOR)
    print()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Demo del ciclo de vida del dato (4 etapas) por trace_id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python -m analytics.trace_lifecycle --symbol BTCUSDT --date 2026-05-26\n"
            "  python -m analytics.trace_lifecycle --symbol ETHUSDT --date 2026-05-26 \\\n"
            "      --trace-id a1b2c3d4-1234-5678-9abc-def012345678\n"
        ),
    )
    parser.add_argument("--symbol", required=True,
                        help="Símbolo (parte de la partition key). Ej: BTCUSDT")
    parser.add_argument("--date", required=True,
                        help="Fecha yyyy-mm-dd (parte de la partition key).")
    parser.add_argument("--trace-id", default=None,
                        help="UUID v4 del evento. Si se omite, se elige uno al "
                             "azar de la partición.")
    args = parser.parse_args()

    # Validación temprana del UUID para fallar antes de tocar Cassandra.
    if args.trace_id is not None:
        try:
            uuid.UUID(args.trace_id)
        except ValueError:
            print(f"❌ --trace-id no es un UUID válido: {args.trace_id}")
            return 2

    try:
        return run(args.symbol, args.date, args.trace_id)
    finally:
        close_session()


if __name__ == "__main__":
    sys.exit(main())
