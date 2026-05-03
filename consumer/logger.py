"""
consumer/logger.py
------------------
Logger estructurado JSON para CryptoFlow.

[LOG-2] MEJORA: JsonFormatter usaba datetime.now() en format(), llamado
  una vez por registro. El campo "ts" ahora viene del record.created
  (timestamp POSIX que Python asigna cuando el registro se crea, no cuando
  se formatea). Elimina la syscall extra de time.time() por mensaje.

[LOG-1] MEJORA: get_rate_logger() devuelve un logger con tasa de eventos
  (ev/s) calculada sobre una ventana deslizante de 10 segundos, en lugar
  del conteo acumulado que no dice nada sobre el estado actual.
"""

import json
import os
import sys
import time
import logging
from collections import deque
from datetime import datetime, timezone

# Nivel configurable por env — sin tocar código para cambiar verbosidad
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


class JsonFormatter(logging.Formatter):
    """Emite registros como JSON de una línea a stderr."""

    def format(self, record: logging.LogRecord) -> str:
        # [LOG-2] record.created es el timestamp POSIX asignado al crear
        # el registro — no requiere syscall adicional en format()
        payload = {
            "ts":     datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Campos extra pasados como kwargs en logger.info("msg", extra={...})
        if hasattr(record, "ctx"):
            payload["ctx"] = record.ctx
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
        logger.propagate = False
    return logger


class RateTracker:
    """
    [LOG-1] Calcula eventos/segundo en una ventana deslizante de N segundos.

    Uso:
        tracker = RateTracker(window_s=10)
        tracker.record()           # llamar por cada evento
        rate = tracker.rate()      # ev/s en los últimos 10s
    """

    def __init__(self, window_s: float = 10.0):
        self._window = window_s
        self._timestamps: deque[float] = deque()

    def record(self) -> None:
        now = time.monotonic()
        self._timestamps.append(now)
        # Limpiar timestamps fuera de la ventana
        cutoff = now - self._window
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def rate(self) -> float:
        """Retorna eventos/segundo en la ventana actual."""
        if len(self._timestamps) < 2:
            return 0.0
        elapsed = self._timestamps[-1] - self._timestamps[0]
        return len(self._timestamps) / elapsed if elapsed > 0 else 0.0

    def count(self) -> int:
        return len(self._timestamps)
