"""
storage/cassandra_writer.py
----------------------------
Escritura eficiente de eventos en Cassandra.

Decisiones de diseño para rendimiento:
─────────────────────────────────────────────────────────────────────────────
1. Prepared statements
   Preparados una sola vez en __init__, reutilizados en cada inserción.
   Cassandra parsea y planifica el statement solo una vez; las ejecuciones
   siguientes solo envían los valores (sin overhead de parsing).

2. execute_async + callbacks
   Cada inserción es no-bloqueante. El driver envía la request y registra
   un callback de error. El consumer sigue procesando eventos sin esperar
   la confirmación de disco de Cassandra.

3. Micro-batching con UNLOGGED BATCH
   Cassandra desaconseja batches grandes (>100 filas, >5KB) porque generan
   presión en el coordinador. La estrategia aquí:
     - Solo se batchean inserciones al MISMO partition key (mismo símbolo+fecha)
     - Máx BATCH_SIZE filas por batch (default: 25)
     - Flush automático cada FLUSH_INTERVAL_S segundos (default: 1.0s)
   Esto maximiza throughput sin violar las recomendaciones de Cassandra.

4. date como string yyyy-mm-dd
   Parte de la partition key. Se calcula una vez por evento desde
   trade_time/event_time (no desde ingestion_ts) para consistencia
   con el tiempo real del mercado.

5. Errores
   Los errores de red o timeout no propagan excepción al consumer.
   Se loguean y se incrementa un contador de errores. El consumer
   nunca se detiene por un fallo de escritura individual.
─────────────────────────────────────────────────────────────────────────────

Flujo:
    consumer → CassandraWriter.write(event) → buffer → flush → Cassandra
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Union

from cassandra.query import BatchStatement, BatchType, BoundStatement

from consumer.models import AggTrade, BookTicker
from consumer.logger import get_logger
from storage.session import get_session

logger = get_logger("cassandra.writer")

# ---------------------------------------------------------------------------
# Configuración de micro-batching
# ---------------------------------------------------------------------------

BATCH_SIZE = 25           # máx statements por batch (mismo partition key)
FLUSH_INTERVAL_S = 1.0    # flush periódico aunque no se alcance BATCH_SIZE


# ---------------------------------------------------------------------------
# Sentencias CQL (se preparan una sola vez)
# ---------------------------------------------------------------------------

_INSERT_TRADE = """
    INSERT INTO cryptoflow.raw_trades (
        symbol, date, trade_time, event_time,
        agg_trade_id, price, quantity,
        is_buyer_maker, trace_id, ingestion_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_BOOK_TICKER = """
    INSERT INTO cryptoflow.raw_book_tickers (
        symbol, date, event_time,
        best_bid_price, best_bid_qty,
        best_ask_price, best_ask_qty,
        spread, trace_id, ingestion_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_to_date(ms_epoch: int) -> str:
    """Convierte timestamp en ms a string yyyy-mm-dd (UTC)."""
    dt = datetime.fromtimestamp(ms_epoch / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def _on_error(exc: Exception) -> None:
    """Callback registrado en execute_async para loguear fallos silenciosamente."""
    logger.error("Error de escritura en Cassandra: %s", exc)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class CassandraWriter:
    """
    Writer asíncrono con micro-batching para AggTrade y BookTicker.

    Uso típico (como handler del consumer):

        writer = CassandraWriter()
        writer.start()

        consumer = BinanceConsumer(handler=writer.write)
        await consumer.run()

        writer.stop()
    """

    def __init__(
        self,
        batch_size: int = BATCH_SIZE,
        flush_interval_s: float = FLUSH_INTERVAL_S,
    ):
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s

        # Buffers por partition key: (symbol, date) → lista de BoundStatements
        self._buffers: dict[tuple, list[BoundStatement]] = defaultdict(list)
        self._lock = threading.Lock()

        # Stats
        self._stats = {
            "trades_written": 0,
            "tickers_written": 0,
            "batches_flushed": 0,
            "errors": 0,
        }

        # Prepared statements (se inicializan en start())
        self._ps_trade = None
        self._ps_ticker = None

        # Flush periódico en background thread
        self._flush_thread: threading.Thread | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Prepara statements y arranca el flush periódico."""
        session = get_session()

        self._ps_trade = session.prepare(_INSERT_TRADE)
        self._ps_ticker = session.prepare(_INSERT_BOOK_TICKER)
        logger.info("Prepared statements listos.")

        self._running = True
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name="cassandra-flusher",
            daemon=True,
        )
        self._flush_thread.start()
        logger.info(
            "CassandraWriter iniciado. batch_size=%d flush_interval=%.1fs",
            self._batch_size, self._flush_interval_s,
        )

    def stop(self) -> None:
        """Detiene el flush periódico y hace flush final."""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)
        self._flush_all()
        logger.info("CassandraWriter detenido. Stats finales: %s", self._stats)

    # ------------------------------------------------------------------
    # Handler público
    # ------------------------------------------------------------------

    async def write(self, event: Union[AggTrade, BookTicker]) -> None:
        """
        Punto de entrada para el consumer.
        Enlaza el evento a un BoundStatement y lo encola en el buffer.
        El flush ocurre automáticamente (por tamaño o por tiempo).
        """
        if isinstance(event, AggTrade):
            stmt = self._bind_trade(event)
            partition = (event.symbol, _ms_to_date(event.trade_time))
        elif isinstance(event, BookTicker):
            stmt = self._bind_ticker(event)
            partition = (event.symbol, _ms_to_date(event.event_time))
        else:
            logger.warning("Tipo de evento desconocido: %s", type(event))
            return

        with self._lock:
            self._buffers[partition].append(stmt)
            # Flush inmediato si el buffer de esta partición alcanza el límite
            if len(self._buffers[partition]) >= self._batch_size:
                self._flush_partition(partition)

    # ------------------------------------------------------------------
    # Binding
    # ------------------------------------------------------------------

    def _bind_trade(self, event: AggTrade) -> BoundStatement:
        return self._ps_trade.bind((
            event.symbol,
            _ms_to_date(event.trade_time),
            event.trade_time,
            event.event_time,
            event.agg_trade_id,
            event.price,
            event.quantity,
            event.is_buyer_maker,
            uuid.UUID(event.trace_id),
            event.ingestion_ts,
        ))

    def _bind_ticker(self, event: BookTicker) -> BoundStatement:
        date = _ms_to_date(event.event_time) if event.event_time else _ms_to_date(
            int(event.ingestion_ts.timestamp() * 1000)
        )
        return self._ps_ticker.bind((
            event.symbol,
            date,
            event.event_time,
            event.best_bid_price,
            event.best_bid_qty,
            event.best_ask_price,
            event.best_ask_qty,
            event.spread,
            uuid.UUID(event.trace_id),
            event.ingestion_ts,
        ))

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def _flush_loop(self) -> None:
        """Background thread: flush periódico de todos los buffers."""
        while self._running:
            time.sleep(self._flush_interval_s)
            self._flush_all()

    def _flush_all(self) -> None:
        """Flush de todos los buffers pendientes."""
        with self._lock:
            partitions = list(self._buffers.keys())

        for partition in partitions:
            with self._lock:
                self._flush_partition(partition)

    def _flush_partition(self, partition: tuple) -> None:
        """
        Envía un UNLOGGED BATCH para una partition key específica.
        Debe llamarse con self._lock adquirido.
        """
        stmts = self._buffers.pop(partition, [])
        if not stmts:
            return

        session = get_session()
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for stmt in stmts:
            batch.add(stmt)

        # Fire-and-forget: el callback loguea errores sin bloquear
        future = session.execute_async(batch)
        future.add_errback(_on_error)

        # Actualizar stats (aproximado, sin esperar confirmación)
        self._stats["batches_flushed"] += 1
        logger.debug(
            "Batch enviado. partition=%s statements=%d total_batches=%d",
            partition, len(stmts), self._stats["batches_flushed"],
        )

    def get_stats(self) -> dict:
        return dict(self._stats)
