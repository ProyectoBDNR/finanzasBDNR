# CryptoFlow Analytics

CryptoFlow Analytics es un sistema distribuido de análisis de mercados de criptomonedas que captura, almacena, procesa y analiza datos de trading en tiempo real provenientes de Binance. Implementa una arquitectura NoSQL de dos capas, una operativa (Apache Cassandra) optimizada para ingesta de alta velocidad, y una analítica (Apache Spark) para procesamiento distribuido y generación de features cuantitativas, con el objetivo de construir un Feature Engine funcional que transforme eventos crudos de mercado en información estratégica.

---

## Tabla de Contenidos

1. [Stream de Datos](#1-stream-de-datos)
2. [Arquitectura del Sistema](#2-arquitectura-del-sistema)
3. [Modelo de Datos](#3-modelo-de-datos)
4. [Justificación CAP](#4-justificación-cap)
5. [Control de Accesos (RBAC)](#5-control-de-accesos-rbac)
6. [Instalación y Uso](#6-instalación-y-uso)
7. [Estructura del Proyecto](#7-estructura-del-proyecto)
8. [Pipeline de Datos](#8-pipeline-de-datos)
9. [Feature Engineering](#9-feature-engineering)
10. [Consultas de Valor](#10-consultas-de-valor)
11. [Trazabilidad y Evolución del Dato](#11-trazabilidad-y-evolución-del-dato)
12. [Dashboard](#12-dashboard)
13. [Tests](#13-tests)
14. [Hallazgos](#14-hallazgos)
15. [Consideraciones Éticas](#15-consideraciones-éticas)

---

## 1. Stream de Datos

### Resumen

El sistema consume datos en tiempo real del mercado de criptomonedas a través de dos streams complementarios de Binance: trades ejecutados (`aggTrade`) y estado del order book (`bookTicker`). Juntos proporcionan una vista completa de la actividad de mercado, capturando tanto transacciones realizadas como la liquidez disponible en cada momento, para tres de los pares más líquidos del ecosistema cripto: BTCUSDT (Bitcoin), ETHUSDT (Ethereum) y BNBUSDT (BNB).

### Origen y Autoría

Binance es el exchange de criptomonedas con mayor volumen de trading a nivel global. Es responsable de la recolección, distribución y mantenimiento de los datos de trading que se generan en su plataforma. Los datos se distribuyen de forma pública y gratuita a través de su API de WebSocket. No se requiere autenticación para acceder a los streams de datos de mercado.


### APIs y Documentación

| Fuente | URL | Descripción |
|--------|-----|-------------|
| Binance WebSocket | https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams | Streams de datos de mercado en tiempo real |
| Binance aggTrade | https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#aggregate-trade-streams | Trades agregados por símbolo |
| Binance bookTicker | https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-book-ticker-streams | Mejor bid/ask en tiempo real |

### Endpoint WebSocket combinado

```
wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/ethusdt@aggTrade/ethusdt@bookTicker/bnbusdt@aggTrade/bnbusdt@bookTicker
```

### Pares monitoreados

| Símbolo | Activo | Caudal estimado | Justificación |
|---------|--------|-----------------|---------------|
| BTCUSDT | Bitcoin | ~5–50 eventos/s | Mayor liquidez global, referencia del mercado |
| ETHUSDT | Ethereum | ~3–30 eventos/s | Ecosistema DeFi, segundo activo por capitalización |
| BNBUSDT | BNB | ~1–10 eventos/s | Token nativo de Binance, alta actividad en la plataforma |

El caudal combinado de los 6 streams (3 pares × 2 tipos) supera ampliamente el requisito mínimo de ≥1 evento por segundo, alcanzando típicamente entre 10 y 100 eventos por segundo en horarios de alta actividad.

### Diccionario de Datos

#### aggTrade (trades ejecutados)

Cada evento `aggTrade` representa uno o más trades individuales que se ejecutaron al mismo precio en el mismo instante. Binance agrega trades del mismo taker order para reducir el volumen de mensajes.

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `symbol` | string | Par de trading (activo/moneda de cotización) | `BTCUSDT` |
| `agg_trade_id` | long | Identificador único del trade agregado | `3218476549` |
| `price` | float | Precio de ejecución del trade en USDT | `108432.50` |
| `quantity` | float | Cantidad del activo base transaccionada | `0.00523` |
| `trade_time` | long | Timestamp real de ejecución (ms epoch) | `1748210400000` |
| `event_time` | long | Timestamp de emisión del evento por Binance (ms epoch) | `1748210400001` |
| `is_buyer_maker` | boolean | `true` si el comprador fue el maker (orden limit); `false` si fue taker (orden market) | `false` |
| `trace_id` | UUID | Identificador de trazabilidad asignado por el consumer | `a1b2c3d4-...` |
| `ingestion_ts` | timestamp | Momento exacto de recepción por nuestro sistema (UTC) | `2026-05-26T08:35:54Z` |

#### bookTicker (estado del order book)

Cada evento `bookTicker` representa el mejor precio de compra (bid) y venta (ask) disponible en el order book en un instante dado.

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `symbol` | string | Par de trading | `ETHUSDT` |
| `best_bid_price` | float | Mejor precio de compra disponible | `2640.15` |
| `best_ask_price` | float | Mejor precio de venta disponible | `2640.20` |
| `best_bid_qty` | float | Cantidad disponible al mejor precio de compra | `12.450` |
| `best_ask_qty` | float | Cantidad disponible al mejor precio de venta | `8.230` |
| `event_time` | long | Timestamp del evento (ms epoch) | `1748210400005` |
| `spread` | float | Diferencia ask - bid (campo desnormalizado) | `0.05` |
| `trace_id` | UUID | Identificador de trazabilidad | `e5f6g7h8-...` |
| `ingestion_ts` | timestamp | Momento de recepción por el sistema | `2026-05-26T08:35:54Z` |

### Clasificación de Variables

**Variables Cuantitativas:** `price`, `quantity`, `trade_time`, `event_time`, `agg_trade_id`, `best_bid_price`, `best_ask_price`, `best_bid_qty`, `best_ask_qty`, `spread`.

**Variables Cualitativas:** `symbol` (categoría del par de trading), `is_buyer_maker` (dirección del trade).

**Texto No Estructurado:** No se procesan atributos de texto libre en esta versión del sistema. Los eventos de mercado son exclusivamente numéricos y categóricos.

**Series Temporales:** `trade_time` (momento real de ejecución del trade), `event_time` (momento de emisión del evento por Binance), `ingestion_ts` (momento de recepción por nuestro sistema). Estas tres marcas temporales permiten medir la latencia end-to-end del pipeline y detectar retrasos entre la ejecución real y la ingestión.

---

## 2. Arquitectura del Sistema

```
┌──────────────────────────────┐
│   Binance WebSocket          │
│  (aggTrade + bookTicker)     │
│  3 pares × 2 streams = 6     │
└──────────────┬───────────────┘
               │  wss://stream.binance.com
               ▼
┌──────────────────────────────┐
│   Python Consumer            │
│  • Conexión WebSocket async  │
│  • Parsing + validación      │
│  • trace_id (UUID)           │
│  • ingestion_ts (UTC)        │
│  • Reconexión automática     │
│  • Estadísticas de caudal    │
└──────────────┬───────────────┘
               │  Escritura directa (prepared statements)
               ▼
┌──────────────────────────────┐
│   Apache Cassandra           │
│   3 nodos (RF=3)             │
│  ┌────────────────────────┐  │
│  │ raw_trades             │  │
│  │ raw_book_tickers       │  │
│  │ TTL 7 días             │  │
│  └────────────────────────┘  │
│  Capa operativa: verdad      │
│  operativa, escritura rápida │
└──────────────┬───────────────┘
               │  Lectura batch (Spark-Cassandra Connector)
               ▼
┌──────────────────────────────┐
│   Apache Spark               │
│  ┌────────────────────────┐  │
│  │ 1. Limpieza + dedup    │  │
│  │ 2. Agregación OHLCV    │  │
│  │    (1m, 5m, 1h)        │  │
│  │ 3. Spread timeseries   │  │
│  └────────────┬───────────┘  │
│               ▼              │
│  ┌────────────────────────┐  │
│  │ Feature Engine         │  │
│  │ • VWAP                 │  │
│  │ • log_return           │  │
│  │ • rolling_volatility   │  │
│  │ • realized_vol         │  │
│  │ • momentum             │  │
│  │ • buy_sell_ratio       │  │
│  │ • OBI                  │  │
│  │ • return_autocorr      │  │
│  └────────────┬───────────┘  │
│               ▼              │
│  ┌────────────────────────┐  │
│  │ Analytics              │  │
│  │ Q1-Q7 + Backtest BSR   │  │
│  └────────────┬───────────┘  │
└───────────────┼──────────────┘
                │  Escritura a tablas analíticas
                ▼
┌──────────────────────────────┐
│   Cassandra (analítica)      │
│  ┌────────────────────────┐  │
│  │ ohlcv_1m / 5m / 1h     │  │
│  │ features_by_window     │  │
│  │ spread_timeseries      │  │
│  └────────────────────────┘  │
└──────────────┬───────────────┘
               │  Export JSON
               ▼
┌──────────────────────────────┐
│   Dashboard HTML             │
│  Candlesticks, KPIs, spread, │
│  latencia, momentum          │
└──────────────────────────────┘
```

---

## 3. Modelo de Datos

### Capa Raw (operativa)

| Tabla | Partition Key | Clustering | TTL | Propósito |
|-------|--------------|------------|-----|-----------|
| `raw_trades` | `(symbol, date)` | `trade_time DESC, agg_trade_id DESC` | 7 días | Eventos aggTrade crudos |
| `raw_book_tickers` | `(symbol, date)` | `event_time DESC, trace_id DESC` | 7 días | Eventos bookTicker crudos |

Justificación del particionado: `symbol` filtra por activo y `date` (yyyy-mm-dd) limita el tamaño de partición a ~4.3M filas/día máximo, dentro del límite recomendado de Cassandra. El clustering DESC coloca los eventos más recientes primero para optimizar queries con `LIMIT`.

### Capa Analítica

| Tabla | Partition Key | Clustering | Propósito |
|-------|--------------|------------|-----------|
| `ohlcv_1m` | `(symbol, window_label)` | `window_start DESC` | Velas de 1 minuto |
| `ohlcv_5m` | `(symbol, window_label)` | `window_start DESC` | Velas de 5 minutos |
| `ohlcv_1h` | `(symbol, window_label)` | `window_start DESC` | Velas de 1 hora |
| `spread_timeseries` | `(symbol, window_label)` | `window_start DESC` | Evolución del spread bid/ask |
| `features_by_window` | `(symbol, window_label)` | `window_start DESC` | Features cuantitativas completas |

La tabla `features_by_window` contiene 29 columnas que representan el dataset final del Feature Engine, incluyendo OHLCV, features calculadas y métricas de spread.

---

## 4. Justificación CAP

Según el teorema CAP, un sistema distribuido solo puede garantizar simultáneamente dos de tres propiedades: Consistencia, Disponibilidad y Tolerancia a Particiones.

CryptoFlow prioriza **AP (Disponibilidad + Tolerancia a Particiones)** sobre consistencia estricta:

**Disponibilidad (A):** El sistema no puede perder eventos de mercado. Si un nodo Cassandra cae, los otros dos siguen aceptando escrituras sin interrupción. En un stream de 10-100 eventos/segundo, cada segundo offline significa datos perdidos irrecuperables.

**Tolerancia a Particiones (P):** Con 3 nodos distribuidos, el sistema tolera fallos de red entre nodos sin detener la ingesta. Cassandra está diseñada para operar en entornos donde las particiones de red son inevitables.

**Consistencia (C):** Se acepta consistencia eventual. Con `RF=3` y `LOCAL_QUORUM`, cada escritura se replica en 3 nodos y requiere confirmación de 2 — un balance pragmático que garantiza que los datos estén disponibles eventualmente en todos los nodos, tolerando 1 fallo de nodo sin pérdida de datos.

**¿Por qué no CP?** Un sistema CP (e.g., PostgreSQL con replicación síncrona) rechazaría escrituras durante particiones de red, causando pérdida de eventos de mercado. Para un stream de trading esto es inaceptable — es preferible tener un dato eventualmente consistente que no tenerlo en absoluto.

---

## 5. Control de Accesos (RBAC)

El sistema implementa control de accesos basado en roles usando `PasswordAuthenticator` y `CassandraAuthorizer` de Cassandra 4.1.

### Roles

| Rol | Tipo | Permisos | Propósito |
|-----|------|----------|-----------|
| `cassandra` | Superusuario | Todos (password cambiado de default a `BDNR`) | Administración del cluster, DDL, gestión de roles |
| `cf_admin` | Admin | SELECT, MODIFY en todas las tablas de `cryptoflow` | Monitoreo y mantenimiento operativo |
| `cf_writer` | Escritura | MODIFY en `raw_trades`, `raw_book_tickers` | Usado por el consumer Python para insertar eventos raw |
| `cf_analyst` | Lectura/Escritura analítica | SELECT/MODIFY en tablas OHLCV, features, spread | Usado por Spark para leer datos raw y escribir resultados analíticos |

### Principio de mínimo privilegio

Cada componente del sistema solo tiene acceso a lo estrictamente necesario. El consumer no puede leer datos analíticos, Spark no puede escribir en tablas raw, y ninguno puede modificar el schema ni gestionar roles. Esto previene daños accidentales y limita el impacto de credenciales comprometidas.

---

## 6. Instalación y Uso

### Requisitos previos

- Docker Desktop instalado y corriendo
- Python 3.10+ con entorno virtual (recomendado)
- ~4 GB RAM disponible para Docker (3 nodos Cassandra + Spark)

### Primera instalación

```bash
# 1. Clonar el repositorio
git clone <https://github.com/ProyectoBDNR/finanzasBDNR>
cd cryptoflow

# 2. Crear entorno virtual e instalar dependencias
python -m venv venv
source venv/bin/activate        # Linux/Mac
.\venv\Scripts\activate         # Windows PowerShell
pip install -r requirements.txt

# 3. Setup automático del cluster (levanta 3 nodos Cassandra + Spark)
python setup.py --reset
```

El script `setup.py` automatiza todo el proceso en ~3 minutos:

1. Levanta `cassandra-1` (seed node) y espera la señal de auth lista en los logs
2. Aplica el DDL (schema de tablas) y RBAC (roles y permisos)
3. Aplica la migración v2 (tabla `features_by_window` con schema completo)
4. Actualiza el `.env` con las credenciales correctas
5. Levanta `cassandra-2` de forma secuencial — espera a que esté en estado UN antes de continuar
6. Levanta `cassandra-3` y Spark — el arranque secuencial evita conflictos de bootstrap simultáneo (`consistent.rangemovement`)
7. Verifica que los 3 nodos estén en estado UN y que los roles RBAC existan

### Uso

```bash
# Pipeline completo: ingesta en tiempo real + procesamiento Spark automático cada 5 min
python main.py

# Procesamiento manual (Spark dentro de Docker)

docker exec -e CASSANDRA_HOSTS=cassandra-1 -e CASSANDRA_ANALYST_USER=cf_analyst -e CASSANDRA_ANALYST_PASSWORD=analyst_pwd_BDNR cryptoflow-spark /app/run_spark.sh /app/processing/job.py --date YYYY-MM-DD

docker exec -e CASSANDRA_HOSTS=cassandra-1 -e CASSANDRA_ANALYST_USER=cf_analyst -e CASSANDRA_ANALYST_PASSWORD=analyst_pwd_BDNR cryptoflow-spark /app/run_spark.sh /app/feature_engine/runner.py --date YYYY-MM-DD

docker exec -e CASSANDRA_HOSTS=cassandra-1 -e CASSANDRA_ANALYST_USER=cf_analyst -e CASSANDRA_ANALYST_PASSWORD=analyst_pwd_BDNR cryptoflow-spark /app/run_spark.sh /app/analytics/run_demo.py --date YYYY-MM-DD

# Dashboard
cd analytics && python -m http.server 8080
# Abrir http://localhost:8080/dashboard.html
```

### Configuración (.env)

```dotenv
CASSANDRA_HOSTS=127.0.0.1          # Para Python en Windows (puerto mapeado)
CASSANDRA_SPARK_HOSTS=cassandra-1  # Para Spark dentro de Docker (hostname interno)
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=cryptoflow
CASSANDRA_WRITER_USER=cf_writer
CASSANDRA_WRITER_PASSWORD=writer_pwd_BDNR
CASSANDRA_ANALYST_USER=cf_analyst
CASSANDRA_ANALYST_PASSWORD=analyst_pwd_BDNR
CASSANDRA_ADMIN_USER=cf_admin
CASSANDRA_ADMIN_PASSWORD=admin_pwd_BDNR
CASSANDRA_SUPER_PASSWORD=BDNR
SPARK_BATCH_INTERVAL_MINUTES=5
```

Nota: Python (host Windows) usa `127.0.0.1` para conectar al puerto 9042 mapeado. Spark (dentro de Docker) usa `cassandra-1` como hostname interno de la red Docker.

---

## 7. Estructura del Proyecto

```
cryptoflow/
├── setup.py                    # Setup automático: cluster + DDL + RBAC
├── main.py                     # Entrypoint: ingesta + scheduler Spark
├── docker-compose.yml          # 3 nodos Cassandra + Spark
├── cassandra.yaml              # Config Cassandra (PasswordAuthenticator)
├── .env                        # Variables de entorno
│
├── consumer/                   # Capa de ingesta
│   ├── binance_ws.py           # WebSocket client async con reconexión
│   ├── models.py               # Dataclasses: AggTrade, BookTicker
│   └── logger.py               # Logger JSON estructurado + RateTracker
│
├── storage/                    # Interfaz con Cassandra
│   ├── session.py              # Singleton de conexión con RBAC
│   ├── schema_manager.py       # Aplica DDL al arrancar
│   └── cassandra_writer.py     # Prepared statements + micro-batch
│
├── processing/                 # Capa de procesamiento (Spark)
│   ├── spark_session.py        # SparkSession con Cassandra Connector
│   ├── cleaner.py              # Limpieza: nulls, duplicados, tipos
│   ├── aggregator.py           # OHLCV 1m/5m/1h + spread timeseries
│   ├── job.py                  # Job principal: clean → aggregate → export
│   └── scheduler.py            # Cron: ejecuta jobs cada N minutos
│
├── feature_engine/             # Cálculo de features
│   ├── features.py             # 9 features cuantitativas puras
│   └── runner.py               # Orquestador del feature engine
│
├── analytics/                  # Análisis y visualización
│   ├── queries.py              # Q1-Q7 consultas analíticas
│   ├── backtest.py             # Backtest de señal BSR
│   ├── run_demo.py             # Runner con queries + backtest
│   ├── export.py               # Exporta JSON para dashboard
│   └── dashboard.html          # Dashboard interactivo
│
├── schemas/                    # DDL y configuración
│   ├── cassandra.cql           # Schema de tablas + keyspace RF=3
│   ├── rbac.cql                # Roles y permisos
│   └── migration_v2.cql       # Migración features_by_window
│
├── infra/                      # Verificación de infraestructura
│   ├── smoke_test.py           # 19 checks de salud del sistema
│   └── verify_rbac.py          # 14 pruebas de permisos RBAC
│
└── tests/                      # Tests unitarios (3,094 líneas)
    ├── test_consumer.py        # Tests del consumer WebSocket
    ├── test_storage.py         # Tests de escritura a Cassandra
    ├── test_processing.py      # Tests de Spark: limpieza + agregación
    ├── test_features.py        # Tests de cada feature calculada
    ├── test_analytics.py       # Tests de Q1-Q7 + backtest
    └── validation/             # Tests de validación de datos
        ├── test_duplicates.py  # Verificación de deduplicación
        ├── test_integrity.py   # Integridad referencial
        ├── test_load.py        # Pruebas de carga
        └── test_features_validation.py  # Rangos válidos de features
```

---

## 8. Pipeline de Datos

El pipeline transforma datos de 3 formas secuenciales, cada una ejecutada como un job de Spark independiente:

### Fase 1: Processing Job (`processing/job.py`)

Lee datos raw de Cassandra, aplica limpieza y genera agregaciones temporales.

```
raw_trades (Cassandra)
  │
  ▼
Limpieza (cleaner.py)
  • Eliminar filas con price/quantity NULL o ≤ 0
  • Deduplicar por (symbol, trade_time, agg_trade_id)
  • Normalizar tipos de datos
  │
  ▼
Agregación OHLCV (aggregator.py)
  • Ventanas de 1 minuto, 5 minutos, 1 hora
  • Para cada ventana: open, high, low, close, volume, trade_count
  • buy_volume y sell_volume separados por is_buyer_maker
  │
  ▼
Spread Timeseries (aggregator.py)
  • Procesa raw_book_tickers
  • Calcula por ventana: spread_mean, spread_min, spread_max, spread_std
  • OBI (Order Book Imbalance): (bid_qty - ask_qty) / (bid_qty + ask_qty)
  │
  ▼
Escritura a Cassandra + Export JSON
  • ohlcv_1m, ohlcv_5m, ohlcv_1h
  • spread_timeseries
  • JSON en analytics/data/ para dashboard
```

### Fase 2: Feature Engine (`feature_engine/runner.py`)

Lee los datos agregados y calcula features cuantitativas.

```
ohlcv_1m + spread_timeseries (Cassandra)
  │
  ▼
Feature Engine (features.py)
  • VWAP, log_return, rolling_volatility
  • realized_volatility (Parkinson), momentum
  • buy_sell_ratio, OBI, return_autocorr
  │
  ▼
Join features + spread
  • Dataset final: 29 columnas
  │
  ▼
Escritura a Cassandra
  • features_by_window
  • spread_timeseries
```

### Fase 3: Analytics (`analytics/run_demo.py`)

Ejecuta las 7 consultas analíticas y el backtest.

```
features_by_window + ohlcv_1h (Cassandra)
  │
  ▼
Queries Q1-Q7 + Backtest BSR
  │
  ▼
Output: resultados en consola + JSON para dashboard
```

### Scheduler automático

`main.py` ejecuta simultáneamente la ingesta WebSocket y un scheduler que lanza los tres jobs de Spark cada 5 minutos (configurable vía `SPARK_BATCH_INTERVAL_MINUTES`).

---

## 9. Feature Engineering

### Features P0 (core)

| Feature | Fórmula | Descripción |
|---------|---------|-------------|
| **VWAP** | `Σ(price × qty) / Σ(qty)` | Precio promedio ponderado por volumen. Benchmark institucional para evaluar calidad de ejecución. |
| **log_return** | `ln(close_t / close_{t-1})` | Retorno logarítmico entre ventanas consecutivas. Base para cálculos de volatilidad y momentum. |
| **rolling_volatility** | `stddev(log_return, N=10)` | Volatilidad histórica: desviación estándar de los últimos 10 log_returns. |
| **momentum** | `close_t - close_{t-5}` | Cambio absoluto de precio en 5 ventanas. |
| **momentum_pct** | `momentum / close_{t-5}` | Momentum como porcentaje del precio base. |
| **trade_count** | `count(trades)` | Número de trades ejecutados en la ventana. Proxy de actividad de mercado. |
| **total_volume** | `Σ(quantity)` | Volumen total transaccionado en la ventana. |
| **buy_sell_ratio** | `buy_volume / sell_volume` | Ratio de presión compradora vs vendedora. BSR > 1 indica dominio comprador. |

### Features P1 (microestructura)

| Feature | Fórmula | Descripción |
|---------|---------|-------------|
| **realized_volatility** | Parkinson: `√(ln(H/L)² / (4·ln2))` | Volatilidad realizada usando rango high-low. Más eficiente que la volatilidad de cierre porque captura la variación intradía. |
| **OBI** | `(bid_qty - ask_qty) / (bid_qty + ask_qty)` | Order Book Imbalance. Valores positivos indican presión compradora en el order book. |
| **return_autocorr** | `corr(ret_t, ret_{t-1})` | Autocorrelación del log_return. Valores significativos indican tendencia (positivo) o reversión a media (negativo). |
| **spread_mean_pct** | `spread_mean / mid_price_mean` | Spread normalizado como porcentaje del precio medio. Permite comparar liquidez entre activos de diferente precio. |

---

## 10. Consultas de Valor

El sistema ejecuta 7 consultas analíticas complejas que responden preguntas de negocio sobre el mercado, más un backtest predictivo:

### Q1 — Régimen de volatilidad por activo

Clasifica cada ventana temporal en régimen LOW, MED o HIGH usando percentiles relativos por símbolo (p33, p66). Detecta transiciones entre regímenes y la duración de cada fase, permitiendo identificar periodos de calma vs. estrés de mercado.

### Q2 — Spread vs. volumen: perfil de liquidez

Segmenta las ventanas en cuartiles de volumen y analiza el spread promedio de cada cuartil. Determina si la liquidez mejora con el volumen: un `spread_vol_ratio` que baja al subir el volumen indica liquidez profunda (más market makers atraídos por la actividad).

### Q3 — Divergencia de momentum cross-asset

Calcula si los tres activos se mueven en la misma dirección o divergen. Genera un indicador de consenso (concordance): 1.0 = todos en la misma dirección, 0.33 = divergencia completa. Permite detectar movimientos independientes vs. correlacionados del mercado.

### Q4 — Anomalías de volumen (distribución t, colas pesadas)

Identifica ventanas con volumen anormalmente alto usando un z-score robusto. Los retornos de criptomonedas tienen kurtosis > 3, por lo que se utiliza una distribución t (colas pesadas) en lugar de distribución normal. Las anomalías se clasifican con el retorno de precio asociado para determinar si el volumen anómalo es alcista o bajista.

### Q5 — Latencia del pipeline

Mide la latencia end-to-end del sistema usando las tres marcas temporales del dato: `trade_time` (ejecución real), `event_time` (emisión por Binance), `ingestion_ts` (recepción en nuestro sistema). Reporta percentiles p50, p95 y p99 de latencia por símbolo, permitiendo validar que el pipeline opera en tiempos aceptables.

### Q6 — Presión compradora acumulada

Calcula la presión neta de compra vs. venta acumulada en una media móvil de 10 ventanas (CMBP: Cumulative Moving Buy Pressure). Permite identificar periodos sostenidos de acumulación (compra) o distribución (venta), que son señales de interés para análisis técnico.

### Q7 — VWAP tracking error

Calcula la desviación del precio de cierre respecto al VWAP en cada ventana. Un tracking error cercano a 0 indica que el mercado se ejecuta eficientemente alrededor del precio ponderado. Tracking errors grandes sugieren presión direccional o ineficiencias de ejecución.

### Backtest BSR (Buy/Sell Ratio como señal predictiva)

Valida si el Buy/Sell Ratio tiene poder predictivo: cuando BSR > umbral (dominio comprador), ¿el precio sube en las siguientes N ventanas? Calcula hit rate (% de aciertos) y forward return promedio para convertir el proyecto de descriptivo a predictivo.

---

## 11. Trazabilidad y Evolución del Dato

El sistema registra el ciclo de vida completo de cada dato, desde el evento crudo capturado en la capa de ingesta hasta la información estratégica en la capa analítica.

### Etapa 1: Evento crudo

Un trade se ejecuta en Binance. Binance emite un evento `aggTrade` con el precio, cantidad y timestamp. Nuestro consumer Python lo recibe, le asigna un `trace_id` (UUID único) y un `ingestion_ts` (momento de recepción), y lo inserta en la tabla `raw_trades` de Cassandra.

```
Evento crudo en raw_trades:
  symbol=BTCUSDT, price=108432.50, quantity=0.00523
  trade_time=1748210400000, event_time=1748210400001
  is_buyer_maker=false
  trace_id=a1b2c3d4-..., ingestion_ts=2026-05-26T08:35:54Z
```

### Etapa 2: Dato limpio y agregado

Spark lee los trades raw, elimina duplicados (por `agg_trade_id`), filtra valores inválidos, y agrega en ventanas temporales de 1 minuto:

```
Ventana OHLCV en ohlcv_1m:
  symbol=BTCUSDT, window_start=2026-05-26T08:35:00
  open=108430.20, high=108445.10, low=108428.50, close=108432.50
  volume=1.234, trade_count=47
  buy_volume=0.821, sell_volume=0.413
```

### Etapa 3: Feature cuantitativa

El Feature Engine calcula indicadores técnicos sobre las ventanas agregadas:

```
Feature en features_by_window:
  symbol=BTCUSDT, window_start=2026-05-26T08:35:00
  vwap=108434.72, log_return=0.000215
  rolling_volatility=3.42e-4, momentum=12.30
  buy_sell_ratio=1.988, spread_mean=0.010
  obi=0.142, regime=LOW
```

### Etapa 4: Información estratégica

Las queries analíticas transforman las features en conclusiones accionables:

```
Q1: BTCUSDT en régimen LOW de volatilidad desde hace 8 ventanas
Q2: La liquidez de BTC mejora 3x cuando el volumen sube al cuartil Q4
Q4: Anomalía de volumen detectada a las 08:35 — z_score=3.2, retorno asociado +0.1%
Backtest: BSR > 2.0 predice retorno positivo con hit rate de 62%
```

### Tres timestamps para medir latencia

| Timestamp | Origen | Significado |
|-----------|--------|-------------|
| `trade_time` | Binance matching engine | Momento real de ejecución del trade |
| `event_time` | Binance WebSocket server | Momento de emisión del evento por Binance |
| `ingestion_ts` | Consumer Python | Momento de recepción en nuestro sistema |

La diferencia `ingestion_ts - trade_time` mide la latencia total del pipeline. La diferencia `event_time - trade_time` mide la latencia interna de Binance. Q5 reporta estos valores como percentiles para cada símbolo.

---

## 12. Dashboard

El dashboard (`analytics/dashboard.html`) es una aplicación web estática que visualiza los resultados del pipeline en tiempo real.

### Cómo funciona

1. Los jobs de Spark exportan JSON a `analytics/data/` (vía `export.py`)
2. El dashboard lee estos JSON con `fetch()` desde el navegador
3. Se auto-refresca cada 5 minutos (alineado con el scheduler de Spark)
4. Usa canvas de HTML5 para renderizar gráficas sin dependencias externas

### Secciones

| Sección | Qué muestra |
|---------|-------------|
| **Header** | Estado del pipeline (live/offline), último batch, selector de símbolo (BTC/ETH/BNB), tabs de resolución temporal (1m/5m/1h), countdown de auto-refresh |
| **KPI Cards** (3) | Para cada símbolo: precio actual, retorno del día, VWAP, volatilidad rolling, BSR, momentum, spread, régimen de volatilidad (LOW/MED/HIGH) |
| **Gráfico de Velas** | Candlesticks OHLCV con colores verde/rojo, wicks, y barras de volumen debajo. Muestra las últimas 60 velas de la resolución seleccionada |
| **Panel de Spread** | Línea de spread_mean con área rellena, estadísticas mín/avg/máx, y OBI (Order Book Imbalance) |
| **Pipeline Health** | Conteos de trades raw, trades limpios, tasa de deduplicación, ventanas OHLCV generadas |
| **Latencia** | Percentiles p50/p95/p99 de latencia Binance y total, por símbolo, con resaltado de valores anómalos |
| **Momentum Cross-Asset** | Barras horizontales comparativas del momentum_pct de los 3 activos, centradas en 0 |

### Para abrir el dashboard

```bash
cd analytics
python -m http.server 8080
# → http://localhost:8080/dashboard.html
```

---

## 13. Tests

El proyecto incluye 3,094 líneas de tests distribuidos en 9 archivos:

| Archivo | Líneas | Cobertura |
|---------|--------|-----------|
| `test_consumer.py` | 152 | Parsing de eventos, reconexión, modelos AggTrade/BookTicker |
| `test_storage.py` | 287 | Escritura a Cassandra, prepared statements, RBAC |
| `test_processing.py` | 374 | Limpieza, deduplicación, agregación OHLCV con Spark |
| `test_features.py` | 314 | Cada feature calculada: VWAP, log_return, volatilidad, BSR |
| `test_analytics.py` | 364 | Queries Q1-Q7, backtest, interpretaciones |
| `test_duplicates.py` | 355 | Verificación end-to-end de deduplicación |
| `test_integrity.py` | 445 | Integridad referencial entre tablas raw y analíticas |
| `test_load.py` | 427 | Pruebas de carga: volumen sostenido sin pérdida |
| `test_features_validation.py` | 376 | Rangos válidos de features (VWAP entre min/max, BSR > 0) |

Adicionalmente, `infra/smoke_test.py` ejecuta 19 checks de salud del sistema (conectividad Cassandra, estado del cluster, tablas existentes) y `infra/verify_rbac.py` ejecuta 14 pruebas de permisos para verificar que cada rol tiene solo los accesos autorizados.

---

## 14. Hallazgos

### 1. Volatilidad concentrada en BTC

BTCUSDT presenta la menor volatilidad rolling promedio de los tres activos, pero concentra los picos más extremos durante eventos macroeconómicos. BNB muestra la mayor variabilidad relativa (~2x la de BTC normalizada por precio), consistente con su menor capitalización y profundidad de mercado.

### 2. El spread mejora con el volumen (liquidez profunda)

El análisis Q2 revela que para los tres activos, el `spread_vol_ratio` decrece significativamente del cuartil Q1 al Q4 de volumen. Esto confirma que la liquidez mejora cuando hay más actividad: más market makers son atraídos por el volumen, reduciendo el spread relativo. BTC tiene los spreads más estables y bajos (~0.01 USDT), mientras BNB presenta los más amplios y variables.

### 3. Momentum en ventanas cortas

Se observan patrones de momentum significativos en ventanas de 1-5 minutos, con concordance cross-asset que varía entre 0.33 (divergencia total) y 1.0 (consenso). La concordance tiende a ser mayor durante movimientos bruscos del mercado, sugiriendo que los tres activos reaccionan de forma sincronizada ante eventos macro pero divergen en condiciones normales.

### 4. Relación volumen-volatilidad

Incrementos en volumen preceden aumentos en volatilidad, especialmente en ETH. Las anomalías de volumen detectadas por Q4 (z_score > 2.0) correlacionan con movimientos de precio de ±0.1-0.5% en las ventanas siguientes, confirmando que el volumen anómalo es un indicador adelantado de movimiento de precio.

### 5. BSR como señal predictiva

El backtest muestra que el Buy/Sell Ratio tiene poder predictivo modesto: cuando BSR > 2.0 (fuerte dominio comprador), el forward return promedio en las siguientes 5 ventanas es positivo. El hit rate varía entre 55-65% dependiendo del activo y el periodo, lo cual es superior al azar (50%) pero insuficiente para un sistema de trading rentable sin considerar costos de transacción.

### 6. Latencia del pipeline

La latencia total del pipeline (trade_time → ingestion_ts) se mantiene consistentemente por debajo de 500ms en p50 para los tres activos, con picos de hasta 1-2 segundos en p99. La latencia de Binance (trade_time → event_time) contribuye típicamente 1-5ms, confirmando que la mayor parte de la latencia es de red entre Binance y nuestro sistema.

---

## 15. Consideraciones Éticas

**Datos públicos, sin información personal.** Todos los datos procesados son datos de mercado públicos emitidos por Binance. No se captura ni almacena información personal identificable (PII) de ningún trader o usuario.

**Uso exclusivamente académico.** Este sistema no ejecuta órdenes de trading reales. Es un proyecto académico para la materia de Bases de Datos No Relacionales. Los resultados del backtest y las features calculadas no constituyen recomendaciones de inversión.

**Sesgo de supervivencia.** Solo se monitorean activos que están listados actualmente en Binance. Activos que fueron de-listados o fracasaron no están representados, lo que puede sesgar las conclusiones sobre el rendimiento general del mercado cripto.

**Sesgo de exchange.** Los datos provienen exclusivamente de Binance, que representa una fracción del volumen global. El comportamiento de los precios y spreads en otros exchanges puede diferir significativamente, especialmente para activos con menor liquidez.

**Volatilidad extrema y riesgo financiero.** Los mercados de criptomonedas son extremadamente volátiles. Los hallazgos y features del sistema no son consejo financiero. Cualquier persona que utilice estos datos o señales para tomar decisiones de inversión lo hace bajo su propia responsabilidad.

**Contexto regulatorio.** El mercado de criptomonedas opera en un entorno regulatorio complejo y variable por jurisdicción. La fuente de datos (Binance) enfrenta restricciones en algunas regiones. Este proyecto no evalúa la legalidad del trading de criptomonedas en ninguna jurisdicción.

---

## Tech Stack

| Componente | Tecnología | Versión |
|------------|-----------|---------|
| Ingesta | Python (asyncio + websockets) | 3.10+ |
| Base de datos operativa | Apache Cassandra | 4.1 |
| Motor de procesamiento | Apache Spark | 3.4.4 |
| Conector Spark-Cassandra | DataStax Connector | 3.4.0 |
| Dashboard | HTML5 + Canvas (sin dependencias) | — |
| Contenedorización | Docker + Docker Compose | — |

---

*BDNR · Primavera 2026 · Datos reales, sin consejo financiero.*