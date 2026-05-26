"""
analytics/export.py
--------------------
Exporta resultados del pipeline a JSON para el dashboard HTML.

Escribe archivos en analytics/data/ que el dashboard lee con fetch().
Se llama al final de cada Spark batch en processing/job.py.

Archivos generados:
    analytics/data/meta.json          ← timestamp, conteos, estado del batch
    analytics/data/ohlcv_1m.json      ← últimas N ventanas de precio/volumen
    analytics/data/spread.json        ← spread timeseries
    analytics/data/features.json      ← features por símbolo (VWAP, vol, momentum)
    analytics/data/latency.json       ← percentiles de latencia
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Directorio de salida — relativo a la raíz del proyecto
DATA_DIR = Path(__file__).parent / "data"


def _ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write(filename: str, data: Any) -> None:
    path = DATA_DIR / filename
    path.write_text(json.dumps(data, default=str, ensure_ascii=False), encoding="utf-8")


def _df_to_records(df: DataFrame, max_rows: int = 500) -> list[dict]:
    """Convierte un DataFrame a lista de dicts serializables."""
    rows = df.limit(max_rows).collect()
    records = []
    for row in rows:
        d = {}
        for k, v in row.asDict().items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif v is None:
                d[k] = None
            else:
                d[k] = v
        records.append(d)
    return records


def export_meta(date: str, trades_raw: int, tickers_raw: int,
                trades_clean: int, tickers_clean: int,
                ohlcv_windows: int) -> None:
    """Metadatos del último batch ejecutado."""
    _ensure_dir()
    _write("meta.json", {
        "date": date,
        "batch_ts": datetime.now(timezone.utc).isoformat(),
        "trades_raw": trades_raw,
        "tickers_raw": tickers_raw,
        "trades_clean": trades_clean,
        "tickers_clean": tickers_clean,
        "dedup_trades": trades_raw - trades_clean,
        "dedup_tickers": tickers_raw - tickers_clean,
        "dedup_rate_pct": round((trades_raw - trades_clean) / max(trades_raw, 1) * 100, 2),
        "ohlcv_windows": ohlcv_windows,
    })


def _export_ohlcv_window(df: DataFrame, label: str, n_windows: int) -> None:
    """Exporta las últimas N ventanas de OHLCV para un label dado (1m, 5m, 1h)."""
    if df is None:
        return
    w = Window.partitionBy("symbol").orderBy(F.col("window_start").desc())
    df_out = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") <= n_windows)
        .drop("_rn")
        .orderBy("symbol", "window_start")
    )
    records = _df_to_records(df_out, max_rows=n_windows * 3)
    by_symbol: dict[str, list] = {}
    for r in records:
        by_symbol.setdefault(r["symbol"], []).append(r)
    _write(f"ohlcv_{label}.json", {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "label": label,
        "symbols": by_symbol,
    })


def export_ohlcv(df_ohlcv_1m: DataFrame,
                 df_ohlcv_5m: DataFrame = None,
                 df_ohlcv_1h: DataFrame = None) -> None:
    """
    Exporta OHLCV para las tres resoluciones temporales.
    1m → últimas 120 ventanas (~2h de datos)
    5m → últimas 144 ventanas (~12h de datos)
    1h → últimas 48 ventanas (~2 días de datos)
    """
    _ensure_dir()
    _export_ohlcv_window(df_ohlcv_1m, "1m", 120)
    _export_ohlcv_window(df_ohlcv_5m, "5m", 144)
    _export_ohlcv_window(df_ohlcv_1h, "1h", 48)


def export_spread(df_spread: DataFrame, n_windows: int = 120) -> None:
    """Exporta spread timeseries para el perfil de liquidez (Q2)."""
    _ensure_dir()

    if df_spread is None or df_spread.count() == 0:
        _write("spread.json", {"updated_at": datetime.now(timezone.utc).isoformat(),
                               "symbols": {}})
        return

    w = Window.partitionBy("symbol").orderBy(F.col("window_start").desc())
    df = (
        df_spread
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") <= n_windows)
        .drop("_rn")
        .orderBy("symbol", "window_start")
    )

    records = _df_to_records(df, max_rows=n_windows * 3)
    by_symbol: dict[str, list] = {}
    for r in records:
        by_symbol.setdefault(r["symbol"], []).append(r)

    _write("spread.json", {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": by_symbol,
    })


def export_features(df_final: DataFrame, label: str = "1m") -> None:
    """
    Exporta features agregadas por símbolo para los KPIs del dashboard.
    label: "1m", "5m" o "1h" — determina el nombre del archivo de salida.
    """
    _ensure_dir()
    filename = f"features_{label}.json"

    if df_final is None or df_final.count() == 0:
        _write(filename, {"updated_at": datetime.now(timezone.utc).isoformat(),
                          "label": label, "symbols": {}})
        return

    # Último valor de cada feature por símbolo
    w_last = Window.partitionBy("symbol").orderBy(F.col("window_start").desc())
    df_last = (
        df_final
        .withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Estadísticas del día
    df_stats = df_final.groupBy("symbol").agg(
        F.round(F.avg("vwap"),               4).alias("vwap_avg"),
        F.round(F.avg("rolling_volatility"), 8).alias("vol_avg"),
        *(
            [F.round(F.avg("spread_mean"), 6).alias("spread_avg")]
            if "spread_mean" in df_final.columns
            else [F.lit(None).alias("spread_avg")]
        ),
        F.round(F.avg("buy_sell_ratio"),     4).alias("bsr_avg"),
        F.count("*").alias("windows"),
        F.first("close", ignorenulls=True).alias("close_first"),
        F.last("close",  ignorenulls=True).alias("close_last"),
    ).withColumn(
        "day_return_pct",
        F.round((F.col("close_last") - F.col("close_first"))
                / F.col("close_first") * 100, 4)
    )

    last_records = _df_to_records(df_last)
    stat_records = _df_to_records(df_stats)

    last_by_sym = {r["symbol"]: r for r in last_records}
    stat_by_sym = {r["symbol"]: r for r in stat_records}

    # Régimen de volatilidad actual
    pct_df = df_final.groupBy("symbol").agg(
        F.percentile_approx("rolling_volatility", 0.33).alias("p33"),
        F.percentile_approx("rolling_volatility", 0.66).alias("p66"),
    )
    df_regime = df_final.join(F.broadcast(pct_df), on="symbol", how="left")
    df_regime = df_regime.withColumn(
        "vol_regime",
        F.when(F.col("rolling_volatility").isNull(), "UNKNOWN")
         .when(F.col("rolling_volatility") <= F.col("p33"), "LOW")
         .when(F.col("rolling_volatility") <= F.col("p66"), "MED")
         .otherwise("HIGH")
    )
    regime_dist = df_regime.groupBy("symbol", "vol_regime").count()
    regime_records = _df_to_records(regime_dist)
    regime_by_sym: dict[str, dict] = {}
    for r in regime_records:
        regime_by_sym.setdefault(r["symbol"], {})[r["vol_regime"]] = r["count"]

    combined: dict[str, dict] = {}
    for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
        combined[sym] = {
            "last": last_by_sym.get(sym, {}),
            "stats": stat_by_sym.get(sym, {}),
            "regime": regime_by_sym.get(sym, {}),
        }

    _write(filename, {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": combined,
    })


def export_latency(df_raw_trades: DataFrame) -> None:
    """Exporta percentiles de latencia por símbolo (Q5)."""
    _ensure_dir()

    if df_raw_trades is None or df_raw_trades.count() == 0:
        _write("latency.json", {"updated_at": datetime.now(timezone.utc).isoformat(),
                                "rows": []})
        return

    df = df_raw_trades.withColumn(
        "binance_latency_ms",
        F.col("event_time") - F.col("trade_time")
    ).withColumn(
        "total_latency_ms",
        (F.unix_timestamp(F.col("ingestion_ts")) * 1000).cast("long")
        - F.col("trade_time")
    ).filter(F.col("binance_latency_ms") >= 0)

    result = df.groupBy("symbol").agg(
        F.percentile_approx("binance_latency_ms", 0.50).alias("binance_p50_ms"),
        F.percentile_approx("binance_latency_ms", 0.95).alias("binance_p95_ms"),
        F.percentile_approx("binance_latency_ms", 0.99).alias("binance_p99_ms"),
        F.percentile_approx("total_latency_ms",   0.50).alias("total_p50_ms"),
        F.percentile_approx("total_latency_ms",   0.95).alias("total_p95_ms"),
        F.percentile_approx("total_latency_ms",   0.99).alias("total_p99_ms"),
        F.count("*").alias("sample_count"),
    ).orderBy("symbol")

    _write("latency.json", {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rows": _df_to_records(result),
    })