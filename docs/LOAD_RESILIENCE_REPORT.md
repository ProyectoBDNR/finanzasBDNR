# Reporte de carga y resiliencia — CryptoFlow

> Cumple la Etapa 3 del proyecto: *(a) demostrar mediante pruebas de carga
> que la base de datos soporta el volumen de entrada sin pérdida de mensajes*
> y *(b) demostrar que el sistema posee alta disponibilidad y la capacidad
> de recuperarse automáticamente ante fallos críticos*.

Este documento describe la metodología, el entorno y los resultados
esperados de dos pruebas complementarias:

1. **Carga del consumer/writer aislado** — `tests/validation/test_load.py`.
   Mide el throughput de los componentes en proceso (parser, buffer del
   writer, deduplicación), sin tocar Cassandra real.
2. **Chaos test sobre el cluster** — `infra/chaos_test.py`.
   Mide el throughput contra el cluster real (RF=3, LOCAL_QUORUM), mata
   un nodo a mitad del test y reporta cuántos eventos se perdieron y
   cuánto tarda la recuperación.

Las dos pruebas se complementan: la primera demuestra que el código
sostiene la carga sin pérdida cuando el destino es ideal; la segunda
demuestra que el destino real sostiene la carga sin pérdida cuando un
nodo falla.

---

## 1. Entorno de prueba

| Componente            | Valor                                                            |
|-----------------------|------------------------------------------------------------------|
| Cluster               | 3 nodos Cassandra 4.1 (`cryptoflow-cassandra-1..3`)              |
| Topología             | `NetworkTopologyStrategy`, `datacenter1: 3`, RF = 3              |
| Consistencia escritura| `LOCAL_QUORUM` (`CASSANDRA_OUTPUT_CONSISTENCY` en `.env`)        |
| Quorum                | ⌈3/2⌉ + 1 = 2 réplicas confirman cada `INSERT`                   |
| Tolerancia            | 1 nodo caído → cluster sigue aceptando escrituras                |
| Heap por nodo         | `HEAP_NEWSIZE=128M`, `MAX_HEAP_SIZE=512M` (ver `docker-compose.yml`) |
| Orquestación          | `docker compose up -d` desde la raíz del repo                    |
| Driver                | `cassandra-driver` (Python), `TokenAwarePolicy` + `DCAwareRoundRobinPolicy` |
| Tabla bajo prueba     | `cryptoflow.raw_trades` (TTL 7 días, ver `schemas/cassandra.cql`)|
| Hardware del runner   | _(rellenar: CPU, RAM, SO al ejecutar)_                           |

---

## 2. Metodología

### 2.1 Carga aislada — `tests/validation/test_load.py`

Mide tres throughputs en proceso, sin red:

| Métrica                       | Objetivo            | Cómo                                              |
|-------------------------------|---------------------|---------------------------------------------------|
| `consumer_throughput`         | ≥ 1 000 ev/s        | Parsear 10 000 payloads JSON sintéticos.          |
| `writer_buffer_throughput`    | ≥ 500 ev/s          | `await writer.write(event)` con Cassandra mockeada.|
| `dedup_at_scale`              | 100 k regs < 2 s    | Hash-set sobre `(symbol, agg_trade_id)`.          |
| `memory_footprint`            | buffer = 0 tras flush | Ciclos repetidos de `write → _flush_all`.       |

El stream real de Binance produce ≈ 90 ev/s (3 símbolos × 2 streams). Los
objetivos están un orden de magnitud por encima para garantizar margen.

Ejecutar con:

```bash
pytest tests/validation/test_load.py -v -s
```

Es deliberadamente **complementario** al chaos test: aquí no hay Cassandra
real, así que no responde la pregunta "¿qué pasa si un nodo cae?". Esa la
responde la sección 2.2.

### 2.2 Chaos test — `infra/chaos_test.py`

Ejecuta tres fases consecutivas con `--workers N` hilos insertando en
`raw_trades` con CL=LOCAL_QUORUM:

| Fase       | Duración (default) | Acción                                                    |
|------------|--------------------|-----------------------------------------------------------|
| `baseline` | 30 s               | Insertar con los 3 nodos UN. Throughput base.             |
| `failure`  | 30 s               | `docker stop cryptoflow-cassandra-2` + seguir insertando. |
| `recovery` | 30 s               | `docker start` del nodo, espera `UN` en `nodetool status`, mide throughput post-recuperación. |

Por cada fase se cuenta:

- **`inserts_ok`** — el coordinador devolvió ACK (≥ 2 réplicas confirmaron).
- **`inserts_failed`** — excepción del driver (timeout, no hay quorum, etc.).
- **`throughput_eps`** = `inserts_ok / duration_s`.
- **`loss_pct`** = `failed / (ok + failed)` × 100.

El tiempo de recuperación se mide haciendo polling de
`nodetool status` (en el seed) hasta que el nodo target aparezca como `UN`,
con timeout configurable (default 120 s).

Cada evento se loguea como una línea JSON en stdout. El resumen final es
otra línea JSON con `level=INFO msg=summary`, fácil de parsear con `jq`.

### 2.3 Reproducir

```bash
# Cluster arriba:
docker compose up -d
# Esperar a que los 3 nodos estén UN (90s aprox tras `up`):
docker exec cryptoflow-cassandra-1 nodetool status

# Test real (necesita el cluster):
python infra/chaos_test.py | tee docs/chaos_run.log

# Smoke test sin cluster (no toca docker, no escribe a Cassandra):
python infra/chaos_test.py --dry-run --baseline-secs 5 --failure-secs 5

# Flags útiles:
python infra/chaos_test.py --node cryptoflow-cassandra-3 --workers 8
```

El script es idempotente: si lo interrumpes con `Ctrl+C` entre el
`docker stop` y el `docker start`, el bloque `finally` reinicia el nodo
de todas formas, dejando el cluster en su estado original.

---

## 3. Resultados esperados

> **Nota:** los valores en cursiva son objetivos de diseño. El usuario debe
> ejecutar el script en su entorno y reemplazar las celdas con los números
> reales — `jq '.msg=="summary"' docs/chaos_run.log` extrae el resumen.

### 3.1 Carga aislada (de `test_load.py`)

| Métrica                        | Objetivo        | Observado |
|--------------------------------|-----------------|-----------|
| `aggTrade` parser              | ≥ 1 000 ev/s    | _______   |
| `bookTicker` parser            | ≥ 1 000 ev/s    | _______   |
| Mixed stream parser            | ≥ 1 000 ev/s    | _______   |
| Writer buffer throughput       | ≥ 500 ev/s      | _______   |
| Dedup 100 k registros          | < 2 s           | _______   |
| Buffer vacío tras `_flush_all` | sí              | _______   |

### 3.2 Chaos test (de `infra/chaos_test.py`)

| Fase       | Throughput esperado (ev/s) | Inserts fallidos esperados | Observado |
|------------|----------------------------|----------------------------|-----------|
| `baseline` | _≈ 1 500 – 3 000 ev/s_ (depende de hardware y `--workers`) | 0 | _______ |
| `failure`  | _80 – 100 % del baseline_  | 0 _(ver §4)_               | _______   |
| `recovery` | _≈ baseline_               | 0                          | _______   |

| Otra métrica                       | Esperado                          | Observado |
|------------------------------------|-----------------------------------|-----------|
| `events_lost_total`                | **0** (garantizado por RF=3 + QUORUM) | _______ |
| `recovery_time_to_un_s`            | _< 60 s_ tras `docker start`      | _______   |
| `failure_vs_baseline_ratio`        | _≥ 0.80_                          | _______   |
| `recovery_vs_baseline_ratio`       | _≈ 1.00_                          | _______   |

---

## 4. Interpretación

### 4.1 Por qué no hay pérdida de mensajes durante el fallo

La tabla `raw_trades` vive en el keyspace `cryptoflow`, definido con
`NetworkTopologyStrategy {'datacenter1': 3}` (`schemas/cassandra.cql:19-30`).
Esto significa que **cada fila se replica en los 3 nodos del cluster**.

El writer escribe con `LOCAL_QUORUM`. Por definición, `LOCAL_QUORUM` para
RF=3 requiere `⌈3/2⌉ + 1 = 2` réplicas que confirmen. Cuando matamos
un nodo:

- Quedan 2 réplicas vivas → el quorum (2) sigue alcanzable.
- El coordinador (Cassandra usa cualquier nodo vivo como coordinador
  gracias a `TokenAwarePolicy`) escribe en las 2 réplicas vivas y devuelve
  ACK al cliente.
- La 3ª réplica (la del nodo muerto) se sincroniza vía **hinted handoff**
  cuando el nodo regresa, o vía **read repair** / `nodetool repair` si
  los hints expiran.

Por tanto, las escrituras confirmadas **no se pierden** y los lectores
con CL = `LOCAL_QUORUM` ven datos consistentes en cuanto se restaure el
quorum tras la recuperación.

### 4.2 Qué tipo de fallo modela este test

El test mata a un nodo "no-seed" (`cryptoflow-cassandra-2` por default).
Esto es la falla más común y la que mejor mide tolerancia a fallos de
hardware o de proceso. No cubre:

- **Fallo del seed (`cassandra-1`)**: el cluster se mantiene operativo si
  ya está formado, pero nuevos nodos no podrían unirse hasta que el seed
  vuelva. Ortogonal a la pregunta de pérdida de datos. Probable con
  `python infra/chaos_test.py --node cryptoflow-cassandra-1 --seed cryptoflow-cassandra-2`.
- **Fallo simultáneo de 2 nodos**: rompe el quorum
  (1/3 < 2). `LOCAL_QUORUM` empieza a fallar; se esperan
  `WriteTimeout`/`Unavailable`. Fuera de alcance de RF=3 — requiere RF=5
  para tolerarlo.
- **Partición de red dentro del cluster**: docker stop simula crash, no
  split-brain. Para esto haría falta `iptables` o `tc netem`.

### 4.3 Caveat de throughput

El throughput absoluto que reporte el script está acotado por:
1. **El hardware del runner** (CPU del cliente y de los nodos).
2. **`--workers`** — el cliente es síncrono por hilo. Pocos hilos
   subutilizan la concurrencia del driver; demasiados saturan la CPU del
   cliente antes que la del cluster.
3. **El heap de Cassandra** (configurado bajo: 512 MB por nodo para
   desarrollo local). En producción se esperaría 4-8 GB.

El valor relevante no es el throughput absoluto, sino la **razón
`failure_vs_baseline_ratio`**: si está cerca de 1.0, el cluster absorbió
la pérdida del nodo sin degradación material. Pequeñas caídas (5–20 %)
durante la fase de failure son esperables y se atribuyen al tiempo que
tarda el driver en marcar al nodo muerto como `DOWN` (típicamente 1–2 s).

---

## 5. Anexo: extraer métricas del log

```bash
# Línea de resumen, formateada:
grep '"msg": "summary"' docs/chaos_run.log | tail -1 | jq .

# Solo el throughput por fase:
grep '"msg": "summary"' docs/chaos_run.log | tail -1 \
  | jq '.phases[] | {name, throughput_eps, inserts_failed}'
```

---

## 6. Referencias internas

- `docker-compose.yml` — definición de los 3 nodos.
- `schemas/cassandra.cql:19-30` — `CREATE KEYSPACE` con RF=3.
- `storage/cassandra_writer.py` — writer real del pipeline (usa la misma
  consistencia que este test).
- `tests/validation/test_load.py` — pruebas de carga del código
  (consumer + writer aislados).
- `infra/chaos_test.py` — este chaos test.
