"""
analytics/cql_queries.py
-------------------------
Queries CQL nativas para Cassandra.

Cuándo usar Cassandra directamente vs Spark:

  ┌─────────────────────────────────────────────┬───────────────────────────┐
  │ Usar Cassandra (CQL)                        │ Usar Spark                │
  ├─────────────────────────────────────────────┼───────────────────────────┤
  │ Lookup de un evento por clave exacta        │ Agregaciones cross-symbol │
  │ Últimos N eventos de un símbolo             │ Window functions          │
  │ Rango temporal dentro de una partición      │ Pivots y correlaciones    │
  │ Trazabilidad de un trace_id                 │ Z-score / percentiles     │
  │ Writes de alta frecuencia                   │ Joins entre tablas        │
  └─────────────────────────────────────────────┴───────────────────────────┘

Las queries CQL aquí son ejecutables desde cqlsh o desde cassandra-driver.
Cada una tiene el anti-patrón que evita y por qué.
"""


# ─────────────────────────────────────────────────────────────────────────────
# CQL 1 · Últimos N trades de un símbolo en un día específico
# ─────────────────────────────────────────────────────────────────────────────
#
# Caso de uso: inspeccionar los trades más recientes de BTC en tiempo real.
# Eficiente porque:
#   - Filtra exactamente por partition key (symbol, date) → un solo nodo
#   - CLUSTERING ORDER BY trade_time DESC → LIMIT sin sort adicional
#
# Anti-patrón evitado:
#   SELECT * FROM raw_trades WHERE symbol = 'BTCUSDT'  ← sin date = full scan
# ─────────────────────────────────────────────────────────────────────────────

CQL_1_LAST_N_TRADES = """
SELECT symbol, trade_time, price, quantity, is_buyer_maker, trace_id
FROM cryptoflow.raw_trades
WHERE symbol = %(symbol)s
  AND date   = %(date)s
LIMIT %(n)s;
"""
# Uso: {"symbol": "BTCUSDT", "date": "2024-06-10", "n": 50}


# ─────────────────────────────────────────────────────────────────────────────
# CQL 2 · Spread en un rango temporal — ventana de análisis acotada
# ─────────────────────────────────────────────────────────────────────────────
#
# Caso de uso: recuperar los book tickers de ETH en la última hora
# para calcular el spread promedio fuera de Spark (script de monitoreo).
#
# Eficiente porque:
#   - Partition key exacta (symbol, date) → sin scatter
#   - Rango en clustering key (event_time) → Cassandra usa skip de SSTable
#
# Anti-patrón evitado:
#   ALLOW FILTERING en event_time sin date → scan de todas las particiones
# ─────────────────────────────────────────────────────────────────────────────

CQL_2_SPREAD_TIME_RANGE = """
SELECT symbol, event_time, best_bid_price, best_ask_price, spread
FROM cryptoflow.raw_book_tickers
WHERE symbol    = %(symbol)s
  AND date      = %(date)s
  AND event_time >= %(ts_from)s
  AND event_time <= %(ts_to)s;
"""
# Uso: {"symbol": "ETHUSDT", "date": "2024-06-10",
#        "ts_from": 1718010000000, "ts_to": 1718013600000}


# ─────────────────────────────────────────────────────────────────────────────
# CQL 3 · Trazabilidad inversa — dado un trace_id, recuperar el evento raw
# ─────────────────────────────────────────────────────────────────────────────
#
# Caso de uso: un feature anómalo aparece en el dataset analítico.
# Se tiene su trace_id y se quiere ver el evento raw original
# para determinar si es un error de datos o un evento real.
#
# Limitación de Cassandra:
#   trace_id NO está en la partition key, solo en clustering.
#   Esta query requiere conocer (symbol, date) además del trace_id.
#   Sin esos dos campos, habría que usar Spark (full scan distribuido).
#
# Anti-patrón evitado:
#   SELECT * FROM raw_trades WHERE trace_id = ? ← requiere ALLOW FILTERING
# ─────────────────────────────────────────────────────────────────────────────

CQL_3_TRACE_LOOKUP = """
SELECT symbol, trade_time, event_time, price, quantity,
       is_buyer_maker, trace_id, ingestion_ts
FROM cryptoflow.raw_trades
WHERE symbol = %(symbol)s
  AND date   = %(date)s
  AND trade_time = %(trade_time)s;
"""
# Uso: {"symbol": "BTCUSDT", "date": "2024-06-10", "trade_time": 1718017200000}
# Nota: si solo tienes trace_id, usar q5_pipeline_latency en Spark.


# ─────────────────────────────────────────────────────────────────────────────
# CQL 4 · Top trades por tamaño — los 10 trades más grandes del día
# ─────────────────────────────────────────────────────────────────────────────
#
# Caso de uso: identificar "whale trades" — transacciones grandes que
# mueven el mercado. Solo aplicable dentro de una partición (symbol + date).
#
# Limitación importante:
#   Cassandra no puede ordenar por columnas que no son clustering key.
#   "Ordenar por quantity" requiere traer todos los datos y ordenar en cliente.
#   Para un análisis cross-day o cross-symbol, usar Spark (q4_volume_anomalies).
#
# Anti-patrón evitado:
#   ORDER BY quantity DESC ← no soportado en CQL sin clustering key
# ─────────────────────────────────────────────────────────────────────────────

CQL_4_LARGE_TRADES = """
SELECT symbol, trade_time, price, quantity, is_buyer_maker
FROM cryptoflow.raw_trades
WHERE symbol = %(symbol)s
  AND date   = %(date)s;
"""
# Post-procesamiento en Python:
# rows = session.execute(CQL_4_LARGE_TRADES, params)
# top10 = sorted(rows, key=lambda r: r.quantity, reverse=True)[:10]


# ─────────────────────────────────────────────────────────────────────────────
# CQL 5 · Conteo de eventos por símbolo y fecha — health check del pipeline
# ─────────────────────────────────────────────────────────────────────────────
#
# Caso de uso: verificar que el consumer está ingestando datos correctamente.
# Si el conteo de BTC en los últimos 5 minutos baja de lo esperado,
# hay un problema en el pipeline.
#
# Nota: COUNT(*) en Cassandra es eficiente dentro de una partición,
# pero lento en full scan. Siempre filtrar por (symbol, date).
# ─────────────────────────────────────────────────────────────────────────────

CQL_5_PIPELINE_HEALTH = """
SELECT COUNT(*) AS event_count
FROM cryptoflow.raw_trades
WHERE symbol = %(symbol)s
  AND date   = %(date)s
  AND trade_time >= %(ts_from)s;
"""
# Uso: {"symbol": "BTCUSDT", "date": "2024-06-10", "ts_from": ts_5min_ago}


# ─────────────────────────────────────────────────────────────────────────────
# Ejecutor: corre una CQL query contra Cassandra real
# ─────────────────────────────────────────────────────────────────────────────

def execute_cql(query: str, params: dict, session=None):
    """
    Ejecuta una query CQL y retorna los resultados como lista de dicts.

    Args:
        query:   String CQL con placeholders %(name)s
        params:  Dict con los valores de los placeholders
        session: cassandra.cluster.Session (si None, usa get_session())

    Returns:
        Lista de dicts, uno por fila.
    """
    if session is None:
        from storage.session import get_session
        session = get_session()

    rows = session.execute(query, params)
    return [dict(row._asdict()) for row in rows]
