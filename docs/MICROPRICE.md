# Micro-Price

Sección de README — explica la nueva feature `micro_price` y la query Q8.

## ¿Qué es y por qué importa?

El **Micro-Price** es un estimador del "precio justo" del activo en el
Nivel 1 del order book que pondera el bid y el ask por la cantidad
disponible en el lado **contrario**. La intuición: el lado con menos
liquidez es el que tiene más probabilidad de ser barrido por el próximo
trade, así que el precio "verdadero" se desplaza hacia esa cola. Es una
señal estándar en market making y es la base del notebook
[*Market Making with Alpha - Order Book Imbalance*](https://github.com/nkaz001/hftbacktest)
del repo `hftbacktest` (4.1k ⭐, función `obi_mm`).

## Diferencia con OBI

| Métrica       | Tipo                | Rango       | Qué mide                                       |
| ------------- | ------------------- | ----------- | ---------------------------------------------- |
| `obi`         | Ratio adimensional  | `[-1, +1]`  | Asimetría volumétrica relativa del libro       |
| `micro_price` | Precio absoluto USD | (bid, ask)  | Hacia qué lado se desplazará el próximo trade  |

`obi` te dice **cuánto** desbalanceado está el libro; `micro_price` lo
traduce a **dónde** debería cotizar el activo dado ese desbalance.

## Diferencia con `mid_price`

`mid_price = (bid + ask) / 2` es el promedio aritmético: ignora por
completo la asimetría volumétrica. Si hay 100 BTC en el bid y 1 BTC en el
ask, `mid_price` no cambia respecto al caso simétrico. El Micro-Price sí
se desplaza hacia el ask (lado con menor liquidez) porque ahí es donde se
va a "romper" el libro.

Cuando `bid_qty == ask_qty`, `micro_price == mid_price` por construcción.

## Fórmula

```
micro_price = (best_bid * best_ask_qty + best_ask * best_bid_qty)
              / (best_bid_qty + best_ask_qty)

micro_price_divergence_bps =
    ((micro_price - mid_price) / mid_price) * 10_000
```

La divergencia se expresa en **basis points** (1 bps = 0.01%) para que
sea comparable entre activos de muy distinto precio absoluto (BTC ~67k
USD vs BNB ~580 USD).

## Cómo correrlo

```bash
# 1) Crear la tabla destino en Cassandra (idempotente)
docker exec cryptoflow-cassandra-1 cqlsh -u cassandra -p BDNR \
    -f /schemas/migration_v3_microprice.cql

# 2) Calcular y persistir el Micro-Price agregado por ventana
spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    -m feature_engine.microprice
```

El job lee `cryptoflow.raw_book_tickers`, aplica
`compute_micro_price_raw` y `aggregate_micro_price_by_window`, y escribe
en `cryptoflow.microprice_by_window` (PK
`((symbol, window_label), window_start)`).

## Q8 — ¿Predice la divergencia el siguiente retorno?

`analytics/queries_microprice.py` expone
`q8_microprice_divergence_predicts_movement(df_features, df_microprice)`.

Procedimiento:

1. Une `features_by_window` con `microprice_by_window` por
   `(symbol, window_label, window_start)`.
2. Para cada ventana, mira el `log_return` de la ventana **siguiente**
   (con `F.lead`).
3. Bucketiza `micro_price_div_mean_bps` en quintiles por símbolo.
4. Calcula el retorno medio y el hit rate (% de ventanas con
   `forward_return > 0`) por quintile.

### Interpretación del hit rate esperado

- **> 52%** en el quintile 5 (divergencia más positiva) → la señal tiene
  poder predictivo: el Micro-Price anticipa el próximo movimiento.
- **~50%** en todos los quintiles → BTC/ETH/BNB son informacionalmente
  eficientes a 1 minuto y la señal ya está descontada por los market
  makers.
- **Monotonía Q1 → Q5** creciente en `avg_forward_return` refuerza la
  hipótesis incluso si el hit rate global está cerca de 50%, porque
  indica que la magnitud del retorno futuro escala con la divergencia
  aunque el signo se equivoque frecuentemente.

## Referencia

- Repo origen: <https://github.com/nkaz001/hftbacktest>
- Notebook: `examples/Market Making with Alpha - Order Book Imbalance.ipynb`
- Función inspiradora: `obi_mm`
- Paper relacionado: Stoikov, S. (2017). *The micro-price: a high
  frequency estimator of future prices.* Quantitative Finance.
