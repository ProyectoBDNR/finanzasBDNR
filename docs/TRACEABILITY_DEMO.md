# Demo de trazabilidad — ciclo de vida del dato

Este documento describe cómo ejecutar y leer la salida de
[`analytics/trace_lifecycle.py`](../analytics/trace_lifecycle.py), el script
que materializa, en vivo y para un evento real, el ciclo de vida descrito en
el §11 del README:

> evento crudo → dato limpio/agregado → feature cuantitativa → información estratégica

El script responde directamente al requisito de la **Etapa 5** del proyecto:

> *"presentar un análisis comparativo que evidencie el ciclo de vida del dato.
> El equipo debe demostrar cómo el 'evento crudo' fue transformado y
> enriquecido hasta convertirse en 'información estratégica'."*

---

## 1. Mecanismo de auditoría: `trace_id`

Cada evento que entra por [`consumer/binance_ws.py`](../consumer/binance_ws.py)
recibe un `trace_id` (UUID v4) generado en
[`consumer/models.py`](../consumer/models.py) en el momento de la recepción
del WebSocket. Ese UUID se persiste junto al evento en `raw_trades` y
`raw_book_tickers`, y actúa como **identificador inmutable** que permite
auditar cualquier registro derivado más tarde en el pipeline.

Aunque las tablas agregadas (`ohlcv_1m`, `features_by_window`) ya no contienen
el UUID individual — porque agregan cientos de trades en una única fila —
el `trace_id` sigue siendo la puerta de entrada al recorrido: con él se llega
al `trade_time` del evento, y con `trade_time` se identifica la ventana de 1
minuto a la que ese evento contribuyó.

---

## 2. Cómo correr el script

### Pre-requisitos

- Cluster Cassandra arriba (`docker compose up -d`).
- Tablas `raw_trades`, `ohlcv_1m`, `features_by_window` con datos del
  consumer + Spark (correr `python -m main` y luego el feature engine).
- Variables de entorno de RBAC en `.env` (o usar el modo sin auth).

### Sin `trace_id` explícito (elige uno al azar de la partición)

```bash
python -m analytics.trace_lifecycle \
    --symbol BTCUSDT \
    --date   2026-05-26
```

Útil para una demo rápida: el script muestrea hasta 200 trades recientes de la
partición `(BTCUSDT, 2026-05-26)` y elige uno al azar. Sirve cuando solo se
quiere mostrar al evaluador que el recorrido funciona end-to-end.

### Con un `trace_id` específico

```bash
python -m analytics.trace_lifecycle \
    --symbol  BTCUSDT \
    --date    2026-05-26 \
    --trace-id a1b2c3d4-1234-5678-9abc-def012345678
```

Útil para auditoría real: el profesor (o el operador) elige un UUID que vio en
un log, en una métrica anómala, o en un dashboard, y el script muestra
exactamente cómo ese evento atravesó el pipeline.

### Argumentos

| Argumento     | Requerido | Descripción                                                                 |
|---------------|-----------|-----------------------------------------------------------------------------|
| `--symbol`    | sí        | Símbolo de Binance: `BTCUSDT`, `ETHUSDT`, `BNBUSDT`. Parte de la partition key. |
| `--date`      | sí        | Día del evento en formato `yyyy-mm-dd`. Parte de la partition key.          |
| `--trace-id`  | no        | UUID v4 del evento. Si se omite, se elige uno al azar.                      |

> **Por qué `--symbol` y `--date` son obligatorios.** En `raw_trades` el
> `trace_id` no es partition key. Sin `(symbol, date)` la búsqueda forzaría un
> scan distribuido sobre todo el cluster (ver
> [`analytics/cql_queries.py`](../analytics/cql_queries.py), CQL 3). Acotando
> a una partición, el `ALLOW FILTERING` es seguro y rápido (un solo nodo,
> ~ms).

---

## 3. Las 4 etapas en detalle

### Etapa 1 · Evento crudo · `cryptoflow.raw_trades`

Lo que ve el sistema cuando Binance entrega un `aggTrade`: precio, cantidad,
lado del trade (`is_buyer_maker`), tres timestamps (`trade_time`, `event_time`,
`ingestion_ts`) y el `trace_id` asignado por el consumer.

Aquí la **información** es exclusivamente la transaccional: *qué pasó, dónde y
cuándo*. No hay contexto ni interpretación todavía.

### Etapa 2 · Dato limpio y agregado · `cryptoflow.ohlcv_1m`

Spark Structured Streaming lee `raw_trades`, deduplica por `agg_trade_id` y
agrupa los trades en ventanas de 1 minuto, generando un OHLCV clásico (open,
high, low, close, volume) más `buy_volume` / `sell_volume` para distinguir
agresión compradora vs vendedora.

**Pérdida intencional**: los trades individuales se colapsan en estadísticas.
Ya no se ve el trade específico — se ve la ventana a la que perteneció.
A cambio, ganamos densidad: una fila por minuto en lugar de miles de
trades/minuto.

El paso clave es el cálculo
```
window_start = floor(trade_time_ms / 60_000) * 60_000
```
en `_window_start_for_trade()`. Ese floor une el evento crudo con su ventana.

### Etapa 3 · Feature cuantitativa · `cryptoflow.features_by_window`

El Feature Engine ([`feature_engine/features.py`](../feature_engine/features.py))
deriva indicadores técnicos sobre la serie de ventanas:

| Feature              | Significado                                                |
|----------------------|------------------------------------------------------------|
| `vwap`               | Precio promedio ponderado por volumen (referencia de equilibrio). |
| `log_return`         | Retorno logarítmico minuto a minuto.                        |
| `rolling_volatility` | Stddev de log_return en ventana de N=10 minutos.            |
| `realized_volatility`| Estimador Garman-Klass (usa high/low — más eficiente).      |
| `momentum`, `momentum_pct` | Diferencia close − close_lag, absoluta y porcentual.  |
| `buy_sell_ratio`     | `buy_volume / sell_volume` — presión compradora.            |
| `obi`                | Order Book Imbalance — desbalance de liquidez.              |
| `spread_mean`        | Spread bid/ask promedio en la ventana.                      |

Aquí el dato pasa de **descriptivo** (qué pasó) a **derivado** (qué dice eso
sobre el mercado). El `rolling_volatility`, por ejemplo, es justo lo que la
Etapa 4 consumirá.

### Etapa 4 · Información estratégica · Q1 — régimen de volatilidad

La query Q1 (definida en [`analytics/queries.py`](../analytics/queries.py))
clasifica cada ventana en `LOW` / `MED` / `HIGH` usando percentiles **por
símbolo** de la propia distribución de `rolling_volatility`:

- `rolling_volatility ≤ p33` → **LOW**
- `rolling_volatility ≤ p66` → **MED**
- `rolling_volatility > p66`  → **HIGH**

El script reproduce esa lógica en CQL puro (sin Spark) para latencia mínima:
trae todos los `rolling_volatility` del símbolo en ventanas de 1m, calcula
p33/p66 en cliente, y emite un régimen + una lectura accionable
(*"tranquilo, posiciones grandes son seguras"* vs *"recortar tamaño, ampliar
stops"*).

Es el punto del pipeline donde el dato deja de ser una métrica y se vuelve
**decisión** — exactamente lo que la rúbrica pide como "información
estratégica".

---

## 4. Ejemplo de salida esperada

```text
══════════════════════════════════════════════════════════════════════════════
  CICLO DE VIDA DEL DATO · evento crudo → información estratégica
══════════════════════════════════════════════════════════════════════════════
  symbol                 BTCUSDT
  date                   2026-05-26
  trace_id               a1b2c3d4-1234-5678-9abc-def012345678

══════════════════════════════════════════════════════════════════════════════
  ETAPA 1 · Evento crudo (aggTrade de Binance)
  Fuente: cryptoflow.raw_trades
══════════════════════════════════════════════════════════════════════════════
  symbol                 BTCUSDT
  date (partition)       2026-05-26
  agg_trade_id           3214876521
  price                  108432.5000
  quantity               0.005230
  is_buyer_maker         False
  trade_time (ms)        1748210432184
    → trade_time UTC     2026-05-26T08:40:32.184000+00:00
  event_time (ms)        1748210432186
    → event_time UTC     2026-05-26T08:40:32.186000+00:00
  ingestion_ts           2026-05-26 08:40:32.412000+00:00
  trace_id               a1b2c3d4-1234-5678-9abc-def012345678
──────────────────────────────────────────────────────────────────────────────
  Latencia interna Binance (event_time - trade_time): 2 ms

══════════════════════════════════════════════════════════════════════════════
  ETAPA 2 · Dato limpio y agregado (OHLCV 1 minuto)
  Fuente: cryptoflow.ohlcv_1m
══════════════════════════════════════════════════════════════════════════════
  symbol                 BTCUSDT
  window_label           1m
  window_start           2026-05-26 08:40:00+00:00
  window_end             2026-05-26 08:41:00+00:00
──────────────────────────────────────────────────────────────────────────────
  open                   108430.2000
  high                   108445.1000
  low                    108428.5000
  close                  108432.5000
  volume                 1.234500
  trade_count            47
  buy_volume             0.821000
  sell_volume            0.413500

══════════════════════════════════════════════════════════════════════════════
  ETAPA 3 · Feature cuantitativa (indicadores técnicos)
  Fuente: cryptoflow.features_by_window
══════════════════════════════════════════════════════════════════════════════
  symbol                 BTCUSDT
  window_label           1m
  window_start           2026-05-26 08:40:00+00:00
──────────────────────────────────────────────────────────────────────────────
  vwap                   108434.722104
  log_return             0.00021530
  rolling_volatility     0.00034210
  realized_volatility    0.00028760
  momentum               12.300000
  momentum_pct           0.011344
  buy_sell_ratio         1.985496
  spread_mean            0.010000
  spread_mean_pct        0.000009
  mid_price_mean         108432.8050
  obi                    0.142000

══════════════════════════════════════════════════════════════════════════════
  ETAPA 4 · Información estratégica (Q1 · régimen de volatilidad)
  Fuente: queries.q1_volatility_regime
══════════════════════════════════════════════════════════════════════════════
  símbolo                BTCUSDT
  sample size (ventanas) 412
  umbral p33             0.00041800
  umbral p66             0.00072350
  rolling_volatility ahora 0.00034210
──────────────────────────────────────────────────────────────────────────────
  Régimen clasificado: 🟢 LOW
  Lectura accionable: Mercado tranquilo — spreads estrechos, posiciones de
  mayor tamaño son seguras.

══════════════════════════════════════════════════════════════════════════════
  ✓ Trazabilidad completa para trace_id = a1b2c3d4-1234-5678-9abc-def012345678
══════════════════════════════════════════════════════════════════════════════
```

> Los valores numéricos son ilustrativos. En una corrida real cada campo
> proviene directamente de la tabla correspondiente — el script no inventa
> nada, solo formatea lo que Cassandra devuelve.

---

## 5. Manejo de casos límite

El script maneja explícitamente los siguientes escenarios:

| Caso                                      | Comportamiento                                                                          |
|-------------------------------------------|-----------------------------------------------------------------------------------------|
| `--trace-id` con UUID malformado          | Falla temprano con mensaje claro, antes de tocar Cassandra. Exit code `2`.              |
| `--trace-id` no existe en la partición    | Imprime "trace_id no encontrado", sugiere revisar símbolo/fecha y el TTL de 7 días.     |
| Partición `(symbol, date)` vacía          | Imprime "partición vacía", sugiere verificar consumer y fecha.                          |
| Ventana OHLCV no agregada todavía         | Etapa 2 imprime advertencia y continúa — etapas 3 y 4 pueden seguir si hay features.    |
| Feature row faltante                      | Etapa 3 imprime advertencia y continúa — etapa 4 reporta "no se puede clasificar".      |
| Insuficientes datos para p33/p66 (<3 obs) | Etapa 4 reporta régimen `UNKNOWN` con explicación.                                      |

---

## 6. Por qué `cassandra-driver` y no Spark

El script usa el driver nativo de Cassandra (sin Spark) por tres razones:

1. **Latencia**: 3 SELECTs a particiones bien conocidas → decenas de ms. Un
   trabajo Spark cold-start tarda 30-60 s solo en arrancar el JVM, sin sumar
   ejecución.
2. **Caso de uso**: una demo de trazabilidad es interactiva. Lo importante es
   que el evaluador pueda ejecutar el script repetidamente con distintos
   `trace_id`. Spark no escala bien a esa cadencia.
3. **Demostración pedagógica**: muestra que cuando la query encaja con la
   partition key de Cassandra (que es el caso aquí), Spark es innecesario y
   contraproducente. Este reparto está documentado en
   [`analytics/cql_queries.py`](../analytics/cql_queries.py).

Para queries que sí justifican Spark (agregaciones cross-symbol, window
functions, joins) ver [`analytics/queries.py`](../analytics/queries.py).
