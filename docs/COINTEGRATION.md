# Cointegración rolling z-score por pares cripto

## Qué es la cointegración

Dos series temporales no estacionarias `X_t`, `Y_t` están **cointegradas**
si existe una combinación lineal `Y_t − (α + β·X_t)` que sí es
estacionaria (Engle & Granger, 1987). Intuitivamente: aunque los
precios individuales hagan caminata aleatoria, su spread oscila
alrededor de un equilibrio y tiende a re-converger tras perturbaciones.

## Por qué aplica a cripto

BTC, ETH y BNB comparten flujos de orden, narrativa macro y, en gran
medida, el mismo conjunto de holders y market makers. En régimen
normal sus retornos están altamente correlacionados (≈ 0.8 en velas
1m). Pero ante eventos idiosincráticos — un hack de Binance, un
upgrade de Ethereum, un anuncio regulatorio sobre BNB — la simetría
se rompe momentáneamente y emergen oportunidades de stat-arb.

El **z-score rolling** del spread de cointegración es la métrica
estándar para detectar esas divergencias:

```
spread_t   = (cum_hedge_t − (α + β · cum_dom_t)) / (α + β · cum_dom_t) · 100
z_score_t  = (spread_t − μ_spread) / σ_spread
```

donde `(α, β)` se reestiman por OLS sobre la misma ventana rolling de
`N` velas (`N=300` por defecto, ≈ 5 horas a 1m) y `μ`, `σ` se calculan
en esa misma ventana. Valores `|z| > 2` indican una divergencia
≥ 2 desviaciones estándar respecto al equilibrio reciente.

## Cómo correrlo

```python
from pyspark.sql import SparkSession
from feature_engine.cointegration import compute_pairwise_zscore, PAIRS

spark = SparkSession.builder.getOrCreate()

ohlcv_1m = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(table="ohlcv_1m", keyspace="cryptoflow")
    .load()
    .select("symbol", "window_start", "close")
)

zscore = compute_pairwise_zscore(spark, ohlcv_1m, pairs=PAIRS, lookback=300)
zscore.write.format("org.apache.spark.sql.cassandra") \
    .options(table="pairs_zscore_1m", keyspace="cryptoflow") \
    .mode("append").save()
```

La tabla destino se crea con `schemas/migration_v4_cointegration.cql`.

## Q9 — divergencias extremas por par

```python
from analytics.queries_cointegration import q9_cointegration_extreme_divergences

summary = q9_cointegration_extreme_divergences(zscore, abs_threshold=2.0)
summary.show(truncate=False)
```

Por cada par `(sym_dom, sym_hedge)` se reporta:

| columna                    | significado                                                |
|----------------------------|------------------------------------------------------------|
| `n_events`                 | número de ventanas con `\|z\| > umbral`                    |
| `max_abs_z`                | máximo `\|z\|` observado en eventos                        |
| `avg_spread_pct_in_events` | spread promedio (%) durante esas ventanas                  |
| `longest_streak_windows`   | racha más larga de ventanas consecutivas en divergencia    |

### Hipótesis e interpretación esperada

El par mejor cointegrado (BTC-ETH) debería mostrar:

- **Menor** `n_events` (divergencias menos frecuentes).
- **Menor** `longest_streak_windows` (re-convergencia más rápida).
- `max_abs_z` más cercano al umbral (sin colas extremas grandes).

El par BTC-BNB debería mostrar la situación opuesta: más eventos y
rachas más largas. ETH-BNB normalmente queda intermedio.

Si la hipótesis se valida en los datos, el z-score del par BTC-ETH es
la señal accionable más confiable de los tres: divergencias breves y
poco frecuentes que el mercado corrige rápidamente — el escenario
ideal para una estrategia de mean reversion.

## Limitaciones

1. **No probamos cointegración formal.** El método de Engle-Granger
   requiere aplicar el test de Augmented Dickey-Fuller (ADF) sobre el
   residuo del OLS para confirmar que es I(0). En este pipeline
   adoptamos la aproximación pragmática del controlador
   `stat_arb.py` de Hummingbot, que asume cointegración estructural y
   se enfoca en el z-score como señal operativa. Esto es **defendible
   para análisis post-hoc descriptivo**, no para un sistema de trading
   en vivo donde el régimen puede romperse.

2. **OLS no robusto.** Un outlier dentro de la ventana sesga
   `(α, β)`. Versiones más sofisticadas usan regresión robusta
   (Huber, Theil-Sen) o filtran outliers previamente.

3. **Ventana fija.** `lookback=300` mezcla regímenes si el activo
   sufre un cambio estructural durante la ventana. Para producción
   se podría modelar como una mixtura de regímenes (Markov-switching
   cointegration).

4. **Bug clásico de look-ahead.** El módulo usa explícitamente
   `Window.rowsBetween(-(lookback-1), 0)` — la versión "del pasado
   hacia el presente". Si por error se usara `rowsBetween(0,
   lookback-1)` el z-score "vería el futuro" y se volvería un
   oráculo. El test `test_shock_injection_no_lookahead` en
   `tests/test_cointegration.py` está diseñado precisamente para
   cazar esa regresión.

## Referencias

- **Engle, R. F., & Granger, C. W. J. (1987).** "Co-integration and
  Error Correction: Representation, Estimation, and Testing".
  *Econometrica*, 55(2), 251-276.
- **Avellaneda, M., & Lee, J.-H. (2010).** "Statistical Arbitrage in
  the U.S. Equities Market". *Quantitative Finance*, 10(7).
- **Hummingbot stat_arb controller** (Apache 2.0, 5k+ ★):
  <https://github.com/hummingbot/hummingbot/blob/master/controllers/generic/stat_arb.py>
  — método `get_spread_and_z_score` y OLS rolling (líneas ~382-402).
