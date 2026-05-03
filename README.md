# CryptoFlow Analytics

Pipeline distribuido de análisis de mercados de criptomonedas en tiempo real.

**Stack:** Python · Apache Cassandra 4.1 · Apache Spark 3.4.4 · Binance WebSocket · CoinGecko · Docker

---

## Prerrequisitos

| Software | Notas |
|---|---|
| Python 3.8+ | python.org |
| Docker Desktop | Incluye Docker Compose |
| Git | Para clonar el repo |

> **Java NO se necesita instalar.** Spark corre dentro del contenedor Docker.

---

## Instalación — Primera vez

```bash
# 1. Clonar y entrar al proyecto
git clone <URL_DEL_REPO>
cd cryptoflow

# 2. Entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt

# 3. Variables de entorno
cp .env.example .env

# 4. Verificar instalación (sin Docker)
python infra/smoke_test.py
# Esperado: 19/19 checks passed — ALL OK

# 5. Levantar Cassandra + Spark
docker-compose up -d
docker ps   # Esperar (healthy) en cryptoflow-cassandra (~60s)

# 6. Crear tablas en Cassandra
docker cp schemas/cassandra.cql cryptoflow-cassandra:/cassandra.cql
docker exec cryptoflow-cassandra cqlsh -f /cassandra.cql
```

---

## Uso

### Ingesta en tiempo real
```bash
python main.py
# Ctrl+C para detener (hace flush limpio)
```

Verificar datos:
```bash
docker exec cryptoflow-cassandra cqlsh -e "SELECT COUNT(*) FROM cryptoflow.raw_trades"
```

### Procesamiento con Spark

```bash
# Demo con datos sintéticos (no requiere datos en Cassandra)
docker exec cryptoflow-spark /app/run_spark.sh /app/processing/job.py --demo

# Con datos reales (cambiar la fecha)
docker exec -e CASSANDRA_HOSTS=cassandra cryptoflow-spark \
  /app/run_spark.sh /app/processing/job.py --date 2026-05-02

# Feature engine
docker exec cryptoflow-spark /app/run_spark.sh /app/feature_engine/runner.py --demo

# Analytics (7 queries)
docker exec cryptoflow-spark /app/run_spark.sh /app/analytics/run_demo.py
```

### Tests
```bash
python -m pytest tests -q
# Esperado: 232 passed
```

---

## Estructura

```
cryptoflow/
├── consumer/           ← WebSocket + parser + modelos
├── storage/            ← Cassandra session + writer
├── processing/         ← Spark cleaner + aggregator + job
├── feature_engine/     ← VWAP, log_return, volatilidad, momentum
├── enrichment/         ← CoinGecko client
├── analytics/          ← 7 queries analíticas
├── schemas/            ← DDL Cassandra
├── tests/              ← 232 tests
├── infra/              ← smoke_test.py
├── main.py             ← Entrypoint pipeline
├── docker-compose.yml
├── requirements.txt
└── run_spark.sh        ← Launcher Spark en Docker
```

---

## Estado actual

| Componente | Estado |
|---|---|
| Ingesta WebSocket → Cassandra | ✅ Funciona |
| Spark OHLCV con trades reales | ✅ Funciona |
| Spark con book tickers reales | ❌ event_time=0 (ver Issues) |
| Feature engine (demo) | ✅ Funciona |
| Analytics Q1-Q7 (demo) | ✅ Funciona |
| 232 tests | ✅ Pasan |

### Issue conocido: book_tickers con event_time=0

Los book tickers se almacenan con `event_time=0`, causando que el cleaner los filtre. Ver [ISSUES.md](ISSUES.md) para el fix.

---

## Streams monitoreados

| Stream | Símbolos | Datos |
|---|---|---|
| aggTrade | BTC, ETH, BNB | precio, cantidad, dirección |
| bookTicker | BTC, ETH, BNB | bid, ask, spread |

---

## Consideraciones éticas

- Datos públicos de Binance, sin información personal
- Uso académico — no se ejecuta trading real
- Los resultados no son consejos financieros
