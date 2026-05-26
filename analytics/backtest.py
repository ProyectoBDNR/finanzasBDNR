"""
analytics/backtest.py
----------------------
Backtesting simple: ¿el Buy/Sell Ratio (BSR) predice el movimiento
de precio en las siguientes N ventanas?

Convierte el proyecto de descriptivo a predictivo: en lugar de solo
describir qué pasó, valida si una señal del pasado hubiera sido útil
para anticipar el futuro.

Pregunta central
────────────────
Si BSR[t] > umbral (dominio comprador), ¿es log_return[t+1..t+N] positivo
más frecuentemente que el azar? Si sí, el BSR tiene poder predictivo.

Metodología
────────────
1. Clasificar cada ventana como señal = 1 (BSR > umbral) o 0 (BSR <= umbral)
2. Para cada ventana con señal = 1, observar el retorno acumulado en
   las siguientes N ventanas (forward return)
3. Comparar el forward return promedio de ventanas con señal vs sin señal
4. Calcular tasa de acierto (hit rate): % de veces que la señal predijo
   correctamente la dirección del precio

Limitaciones
────────────
- No considera costos de transacción (spread, comisiones)
- No considera slippage en ejecución
- Usa datos de entrenamiento y test mezclados (sin walk-forward)
- Los resultados son ilustrativos, no recomendaciones de trading
- El sistema no ejecuta órdenes reales
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run_bsr_backtest(
    df: DataFrame,
    bsr_threshold: float = 1.2,
    forward_windows: int = 3,
) -> dict[str, DataFrame]:
    """
    Backtest de señal BSR: evalúa si un BSR alto predice retornos positivos.

    Args:
        df:              DataFrame de features con columnas:
                         symbol, window_start, buy_sell_ratio, log_return
        bsr_threshold:   Umbral de BSR para clasificar señal compradora.
                         Default 1.2 → al menos 20% más volumen comprador
                         que vendedor.
        forward_windows: Número de ventanas hacia adelante para medir
                         el retorno posterior a la señal (default: 3).

    Returns:
        Diccionario con tres DataFrames:
          "signals"  → ventanas clasificadas con señal y forward return
          "summary"  → resumen estadístico por símbolo
          "hitrate"  → tasa de acierto de la señal por símbolo
    """
    w_order = Window.partitionBy("symbol").orderBy("window_start")

    # ── 1. Clasificar señales ──────────────────────────────────────────────
    # Señal = 1 cuando BSR supera el umbral (dominio comprador claro)
    # Señal = 0 cuando BSR <= umbral (sin señal o dominio vendedor)
    df_signals = df.filter(
        F.col("buy_sell_ratio").isNotNull() &
        F.col("log_return").isNotNull()
    ).withColumn(
        "signal",
        F.when(F.col("buy_sell_ratio") > bsr_threshold, F.lit(1))
         .otherwise(F.lit(0))
    )

    # ── 2. Calcular forward return ─────────────────────────────────────────
    # Forward return = retorno acumulado en las N ventanas siguientes
    # Usamos lag negativo (lead) para mirar hacia adelante.
    # Cada lead[i] captura el retorno de i ventanas en el futuro.
    # La suma es el retorno total del período de holding.
    for i in range(1, forward_windows + 1):
        df_signals = df_signals.withColumn(
            f"_fwd_{i}",
            F.lead("log_return", i).over(w_order)
        )

    # Retorno acumulado forward (suma de log_returns futuros)
    fwd_cols = [F.col(f"_fwd_{i}") for i in range(1, forward_windows + 1)]
    df_signals = df_signals.withColumn(
        "forward_return",
        F.round(sum(fwd_cols), 8)   # type: ignore[arg-type]
    ).withColumn(
        "forward_direction",
        F.when(F.col("forward_return") > 0, F.lit(1))
         .when(F.col("forward_return") < 0, F.lit(-1))
         .otherwise(F.lit(0))
    ).drop(*[f"_fwd_{i}" for i in range(1, forward_windows + 1)])

    df_signals = df_signals.filter(F.col("forward_return").isNotNull())

    # ── 3. Resumen estadístico por símbolo ─────────────────────────────────
    # Comparar forward return promedio cuando hay señal vs cuando no hay señal
    df_summary = df_signals.groupBy("symbol", "signal").agg(
        F.count("*").alias("n_ventanas"),
        F.round(F.avg("forward_return"),    6).alias("fwd_return_avg"),
        F.round(F.stddev("forward_return"), 6).alias("fwd_return_std"),
        F.round(F.avg("buy_sell_ratio"),    4).alias("bsr_avg"),
        # Tasa de acierto: % de veces que el precio subió tras la señal
        F.round(
            F.sum(F.when(F.col("forward_direction") == 1, 1).otherwise(0)) /
            F.count("*") * 100,
            2
        ).alias("hit_rate_pct"),
    ).orderBy("symbol", "signal")

    # ── 4. Tasa de acierto consolidada por símbolo ─────────────────────────
    df_with_signal = df_signals.filter(F.col("signal") == 1)
    df_hitrate = df_with_signal.groupBy("symbol").agg(
        F.count("*").alias("n_señales"),
        F.round(F.avg("forward_return") * 100, 4).alias("fwd_return_avg_pct"),
        F.round(
            F.sum(F.when(F.col("forward_direction") == 1, 1).otherwise(0)) /
            F.count("*") * 100,
            2
        ).alias("hit_rate_pct"),
        F.round(F.avg("buy_sell_ratio"), 4).alias("bsr_avg_en_señal"),
    ).withColumn(
        "interpretacion",
        F.when(F.col("hit_rate_pct") > 55,
               F.lit("✓ Señal con poder predictivo (>55% acierto)"))
         .when(F.col("hit_rate_pct") > 45,
               F.lit("~ Señal neutral (45-55% — similar al azar)"))
         .otherwise(F.lit("✗ Señal sin poder predictivo (<45% acierto)"))
    ).orderBy("symbol")

    return {
        "signals":  df_signals,
        "summary":  df_summary,
        "hitrate":  df_hitrate,
    }


def print_backtest_results(
    results: dict[str, DataFrame],
    bsr_threshold: float,
    forward_windows: int,
) -> None:
    """Imprime los resultados del backtest de forma legible."""
    print(f"\n{'═'*65}")
    print(f"  Backtest — BSR como señal predictiva")
    print(f"  Umbral BSR: {bsr_threshold}  |  Forward windows: {forward_windows}")
    print(f"{'═'*65}")

    print("\n  ── Resumen por símbolo y tipo de señal")
    print("  Signal=1 → BSR > umbral (comprador)  |  Signal=0 → sin señal")
    results["summary"].show(truncate=False)

    print("  ── Tasa de acierto de la señal compradora")
    results["hitrate"].show(truncate=False)

    print("  ── Interpretación:")
    print(f"  Si hit_rate > 50%: el BSR > {bsr_threshold} tiene poder predictivo")
    print(f"  para el retorno acumulado de las {forward_windows} ventanas siguientes.")
    print(f"  Si hit_rate ≈ 50%: la señal no aporta información (mercado eficiente).")
    print(f"  Si hit_rate < 50%: la señal está invertida (señal contraria útil).")
    print()
