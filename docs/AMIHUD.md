# Amihud Illiquidity Ratio

## Qué es

El **Amihud illiquidity ratio** (Amihud, 2002) mide el *price impact realizado*:
cuántas unidades de retorno absoluto se generan por cada dólar operado en
una ventana de tiempo. En su forma rolling, promedia `|log_return| / volume_usd`
sobre N ventanas y produce una serie temporal de iliquidez por activo.

## Diferencia con spread y con OBI

| Métrica          | Qué captura                          | Cuándo se observa           | Es costo realizado? |
|------------------|---------------------------------------|-----------------------------|---------------------|
| `spread_mean_pct`| Costo cotizado (best ask − best bid) | Antes de la transacción     | No — es ex-ante     |
| `obi`            | Presión latente del libro (bid vs ask)| Antes de la transacción    | No — es señal       |
| `amihud_illiq`   | Price impact por dólar operado       | Después de la transacción   | **Sí — ex-post**    |

El spread y el OBI miden la **liquidez ofrecida** en el libro de órdenes. El
Amihud mide la **liquidez consumida**: cuánto se movió el precio dado el
volumen que realmente se ejecutó. Por eso, en mercados donde la profundidad
es asimétrica o el libro engaña (spoofing, layering), Amihud captura cosas
que spread/OBI no ven.

## Fórmula y unidades

$$
\text{ILLIQ}_t = \frac{1}{N} \sum_{s=t-N+1}^{t} \frac{|\,r_s\,|}{\text{VOL}^{USD}_s}
$$

donde $r_s$ es el log_return de la ventana $s$ y $\text{VOL}^{USD}_s$ es el
volumen en dólares operado en esa ventana.

En este pipeline:

- $r_s$ = `log_return` (`feature_engine.features.add_log_return`)
- $\text{VOL}^{USD}_s$ = `price_qty_sum` = $\sum (\text{price}_i \cdot \text{qty}_i)$
  por ventana, ya en USD (`processing.aggregator.compute_ohlcv`)
- Fallback: si `price_qty_sum` no está disponible (p. ej. al leer
  `features_by_window` persistido — su schema en `migration_v2.cql` no
  incluye `price_qty_sum`), se usa `close * volume` como proxy.
- $N$ = `periods`, por defecto 60 ventanas (1 hora con OHLCV de 1m).

**Escalado**: para legibilidad humana se multiplica por `1e10`, lo que deja
la unidad en aproximadamente **basis points por millón USD operado**. Sin
escalar, los valores típicos de BTC quedan en el orden de `1e-11` y son
difíciles de leer.

## Cómo se integra

`amihud_illiq` se agrega como columna a `features_by_window` **en runtime**,
sin crear tabla nueva en Cassandra. La función
`feature_engine.amihud.add_amihud_illiq(df, periods=60)` recibe el
DataFrame de features y devuelve el mismo DataFrame con la columna nueva.

```python
from feature_engine.amihud import add_amihud_illiq

df_features = add_amihud_illiq(df_features, periods=60)
```

La razón de no persistirlo es operacional: el cálculo depende de la ventana
rolling, que cambia según el horizonte de análisis. Mantenerlo en runtime
permite ajustar `periods` sin migrar schema.

## Q10 — Amihud cross-asset por régimen de volatilidad

Pregunta: ¿cómo cambia la iliquidez cuando sube la volatilidad, y cómo se
ordenan los tres símbolos?

```python
from analytics.queries_amihud import q10_amihud_cross_asset

q10_amihud_cross_asset(df_features).show()
```

Resultado: una fila por `(symbol, vol_regime)` con `illiq_mean`,
`illiq_p50`, `illiq_p95`, `n_windows`.

**Hipótesis a defender**:

1. Dentro de cada símbolo:
   `illiq_mean(HIGH) > illiq_mean(MED) > illiq_mean(LOW)`.
   En regímenes volátiles los market makers retiran cotizaciones y el
   price impact por dólar aumenta.

2. Cross-asset (en cualquier régimen):
   `illiq(BNB) > illiq(ETH) > illiq(BTC)`.
   BTC es el más profundo y por tanto el menos ilíquido. BNB es el menos
   profundo del top-3 de Binance spot.

Si Q10 muestra el ranking esperado y el patrón LOW→HIGH consistente,
estamos midiendo el price impact tal como Amihud lo definió.

## Comparación con Kyle's lambda

Una alternativa común para medir price impact es **Kyle's lambda**
(Kyle, 1985), que se estima por regresión $\Delta p = \lambda \cdot Q$ donde
$Q$ es order flow firmado. Kyle exige datos a nivel tick con clasificación
de buy/sell, lo que es costoso de obtener y reproducir.

**Por qué elegimos Amihud y no Kyle**: Goyenko, Holden y Trzcinka (2009,
*Journal of Financial Economics* 92(2), 153-181) compararon empíricamente
varios proxies de iliquidez sobre datos diarios. Encontraron que **Amihud
domina a Kyle's lambda** sobre datos agregados sin tick-by-tick:

> "Among the low-frequency price impact proxies, Amihud's measure performs
> the best." (Goyenko et al., 2009, p. 161)

Dado que nuestro pipeline trabaja con ventanas OHLCV agregadas (1m, 5m,
1h) y no con order flow firmado tick-by-tick, Amihud es la elección
correcta: mismo poder explicativo con orden de magnitud menos de datos y
sin necesidad de inferir el lado agresor.

## Referencias

- Amihud, Y. (2002). Illiquidity and stock returns: cross-section and
  time-series effects. *Journal of Financial Markets* 5(1), 31-56.
  DOI: [10.1016/S1386-4181(01)00024-6](https://doi.org/10.1016/S1386-4181(01)00024-6)
- Goyenko, R. Y., Holden, C. W., & Trzcinka, C. A. (2009). Do liquidity
  measures measure liquidity? *Journal of Financial Economics* 92(2),
  153-181. DOI: [10.1016/j.jfineco.2008.06.002](https://doi.org/10.1016/j.jfineco.2008.06.002)
- Kyle, A. S. (1985). Continuous auctions and insider trading.
  *Econometrica* 53(6), 1315-1335.
