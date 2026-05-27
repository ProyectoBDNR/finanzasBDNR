# Enriquecimiento con dataset estático

## ¿Qué se agregó y por qué?

La Etapa 4 del proyecto pide cruzar el stream de mercado con **datasets
estáticos** que aporten contexto de negocio. CryptoFlow ahora carga una
tabla pequeña de metadata por símbolo en
[data/static/symbols_metadata.csv](../data/static/symbols_metadata.csv).

Esta tabla cubre los tres activos monitoreados (BTCUSDT, ETHUSDT, BNBUSDT)
con atributos cualitativos que **no se pueden derivar del WebSocket de
Binance**:

| columna | descripción |
|---|---|
| `full_name` | nombre largo del activo (Bitcoin, Ethereum, BNB) |
| `category` | clase de activo (cryptocurrency) |
| `sector` | sub-clasificación (Layer1, Exchange Token, Store of Value…) |
| `listing_date_binance` | fecha de listado del par USDT en Binance |
| `approx_market_cap_usd_billions` | market cap aproximado en USD (mil M) |
| `consensus_mechanism` | mecanismo de consenso del protocolo subyacente |
| `is_exchange_token` | flag booleano: ¿es el token nativo de un CEX? |

Los datos son **reales y verificables** contra Binance, CoinMarketCap y la
documentación de cada protocolo.

## ¿Cómo se hace el join?

El módulo [processing/enrichment.py](../processing/enrichment.py) expone
dos funciones:

- `load_symbols_metadata(spark)` lee el CSV con schema explícito.
- `enrich_features(df_features, spark)` hace un **LEFT JOIN** con clave
  `symbol`, envolviendo la metadata en `F.broadcast()` para forzar un
  **broadcast-hash join**:

  ```python
  df_features.join(
      F.broadcast(df_meta),
      on="symbol",
      how="left",
  )
  ```

  Como la metadata tiene 3 filas (cardinalidad mínima) y las features
  pueden tener miles de ventanas por día, el broadcast evita shuffle
  del lado grande y deja la operación en O(n) sobre las features.

El enriquecimiento es **aditivo**: solo añade columnas nuevas y nunca
modifica las existentes. Si un símbolo no tiene match en la metadata,
las columnas nuevas quedan `NULL` (gracias al `LEFT`).

`processing/job.py` invoca `enrich_features()` justo después de
`final_feature_set()` y antes de `export_features()`. Si el CSV falta
o el join falla, el pipeline continúa con las features sin enriquecer
(try/except con `warning`), preservando el comportamiento previo.

## ¿Qué preguntas analíticas habilita?

Las nuevas columnas permiten cruces que antes eran imposibles con solo
el stream:

1. **¿Cómo se comporta el `buy_sell_ratio` en exchange tokens vs Layer1
   "puros"?** Filtrando por `is_exchange_token`, se puede contrastar
   la presión compradora de BNB (token nativo de su propio exchange,
   sujeto a quemas trimestrales y dinámicas de plataforma) frente a
   BTC y ETH.

2. **¿La volatilidad realizada escala con el market cap?** Agrupando
   por `approx_market_cap_usd_billions` se puede mostrar empíricamente
   que activos más grandes tienden a tener `realized_volatility` menor.

3. **¿El mecanismo de consenso afecta el `spread_mean_pct`?** Comparar
   pares PoW (BTC) vs PoS (ETH) vs PoSA (BNB) sobre el spread relativo
   permite hipotetizar sobre liquidez y profundidad del libro.

4. **¿Hay efecto de "antigüedad de listado"?** Aunque los tres pares
   fueron listados en 2017, la columna queda lista para extender el
   sistema a más símbolos y analizar curvas de madurez.

## Ejemplo de query enriquecida (mock)

Tras el join, una agrupación por `sector` sobre features de 1m:

```python
(
    df_enriched
    .groupBy("sector", "is_exchange_token")
    .agg(
        F.avg("buy_sell_ratio").alias("avg_bsr"),
        F.avg("realized_volatility").alias("avg_rv"),
        F.avg("spread_mean_pct").alias("avg_spread_pct"),
    )
    .show()
)
```

Resultado mock sobre una sesión de 12h:

```
+----------------------------------+-------------------+-------+-------+----------------+
|sector                            |is_exchange_token  |avg_bsr|avg_rv |avg_spread_pct  |
+----------------------------------+-------------------+-------+-------+----------------+
|Layer1 / Store of Value           |false              |1.024  |0.412  |0.00012         |
|Layer1 / Smart Contract Platform  |false              |1.011  |0.487  |0.00019         |
|Exchange Token / Layer1           |true               |1.083  |0.621  |0.00028         |
+----------------------------------+-------------------+-------+-------+----------------+
```

Lectura: en esta ventana mock, el exchange token (BNB) muestra mayor
presión compradora (`avg_bsr > 1.08`), mayor volatilidad realizada y
spread relativo más amplio que los Layer1 "puros" — el tipo de
observación que el cruce con metadata estática hace posible.
