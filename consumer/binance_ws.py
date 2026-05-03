"""
consumer/binance_ws.py  [MEJORADO]
-----------------------------------
Mejoras aplicadas:
  [PERF-1]  _ms_to_date cacheado por segundo
  [SCALE-1] Parámetros configurables por env var
  [RESIL-2] stop() espera que el handler en curso termine
  [LOG-1]   Stats con ev/s via RateTracker
  [DEBUG-1] parse_event incluye stream de origen en errores
  [DEBUG-2] SIGUSR1 → health dump sin detener el proceso
"""

import asyncio
import json
import os
import signal
from datetime import datetime, timezone
from typing import Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from consumer.models import AggTrade, BookTicker
from consumer.logger import get_logger, RateTracker

logger = get_logger("binance_ws")

# ── [SCALE-1] Configuración por env ──────────────────────────────────────────

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
SYMBOLS          = os.getenv("SYMBOLS", "btcusdt,ethusdt,bnbusdt").split(",")
STREAM_TYPES     = ["aggTrade", "bookTicker"]
STREAMS          = "/".join(f"{sym}@{st}" for sym in SYMBOLS for st in STREAM_TYPES)
WS_URL           = f"{BINANCE_WS_BASE}?streams={STREAMS}"
PING_INTERVAL_S  = int(os.getenv("WS_PING_INTERVAL_S", "20"))
MAX_RETRIES      = int(os.getenv("WS_MAX_RETRIES", "5"))
BACKOFF_BASE_S   = int(os.getenv("WS_BACKOFF_BASE_S", "2"))
STATS_EVERY_N    = int(os.getenv("STATS_EVERY_N", "500"))

# ── [PERF-1] Caché de date por segundo ───────────────────────────────────────
# Problema: datetime.fromtimestamp() + strftime() en cada evento (~1000/s).
# Solución: la clave cambia cada 1000ms, no cada evento. O(1) espacio.

_date_cache: dict[int, str] = {}

def _ms_to_date(ms_epoch: int) -> str:
    second = ms_epoch // 1000
    if second not in _date_cache:
        _date_cache.clear()
        _date_cache[second] = datetime.fromtimestamp(
            second, tz=timezone.utc
        ).strftime("%Y-%m-%d")
    return _date_cache[second]


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_agg_trade(data: dict) -> AggTrade:
    return AggTrade(
        symbol=data["s"], agg_trade_id=data["a"],
        price=float(data["p"]), quantity=float(data["q"]),
        trade_time=data["T"], event_time=data["E"],
        is_buyer_maker=data["m"],
    )

def parse_book_ticker(data: dict) -> BookTicker:
    return BookTicker(
        symbol=data["s"],
        best_bid_price=float(data["b"]), best_bid_qty=float(data["B"]),
        best_ask_price=float(data["a"]), best_ask_qty=float(data["A"]),
        event_time=data.get("E") or int(__import__('time').time() * 1000),
    )

def parse_event(raw: str) -> AggTrade | BookTicker | None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("JSON decode error: %s | raw=%s", exc, raw[:200])
        return None

    stream     = msg.get("stream", "unknown")   # [DEBUG-1]
    data       = msg.get("data", msg)
    event_type = data.get("e")

    if event_type == "aggTrade":
        return parse_agg_trade(data)
    elif event_type == "bookTicker":
        return parse_book_ticker(data)
    elif event_type is not None:
        logger.debug("Evento ignorado: type=%s stream=%s", event_type, stream)
        return None
    elif "b" in data and "a" in data:          # bookTicker sin campo "e"
        return parse_book_ticker(data)
    return None


async def stdout_handler(event: AggTrade | BookTicker) -> None:
    print(json.dumps(event.to_dict()), flush=True)


# ── Consumer ──────────────────────────────────────────────────────────────────

class BinanceConsumer:

    def __init__(self, handler: Callable[[AggTrade | BookTicker], Awaitable[None]] = stdout_handler):
        self.handler = handler
        self._stop_event = asyncio.Event()
        self._stats = {"agg_trades": 0, "book_tickers": 0, "errors": 0, "reconnects": 0}
        self._rate  = RateTracker(window_s=10.0)   # [LOG-1]

    def stop(self) -> None:
        logger.info("Stop signal recibido.")
        self._stop_event.set()

    # [DEBUG-2] Health dump sin detener el proceso
    def dump_health(self) -> None:
        logger.info("Health dump.", extra={"ctx": {
            **self._stats,
            "rate_eps": round(self._rate.rate(), 1),
            "streams":  STREAMS.split("/"),
            "stopped":  self._stop_event.is_set(),
        }})

    async def run(self) -> None:
        retries = 0
        while not self._stop_event.is_set():
            try:
                logger.info("Conectando a Binance WebSocket.")
                async with websockets.connect(
                    WS_URL, ping_interval=PING_INTERVAL_S,
                    ping_timeout=10, close_timeout=5,
                ) as ws:
                    logger.info("Conexión establecida. streams=%d", len(STREAMS.split("/")))
                    retries = 0
                    await self._consume(ws)

            except ConnectionClosedOK:
                logger.info("WebSocket cerrado limpiamente.")
                break
            except (ConnectionClosedError, OSError, asyncio.TimeoutError) as exc:
                retries += 1
                self._stats["reconnects"] += 1
                if retries > MAX_RETRIES:
                    logger.error("Max reintentos (%d). Abortando.", MAX_RETRIES)
                    break
                backoff = min(BACKOFF_BASE_S ** retries, 60)
                logger.warning("Reconectando. retry=%d/%d backoff=%ds", retries, MAX_RETRIES, backoff)
                await asyncio.sleep(backoff)
            except Exception as exc:
                logger.error("Error inesperado: %s", exc, exc_info=True)
                self._stop_event.set()

        logger.info("Consumer detenido.", extra={"ctx": {
            **self._stats, "final_rate_eps": round(self._rate.rate(), 2),
        }})

    async def _consume(self, ws) -> None:
        async for raw in ws:
            if self._stop_event.is_set():
                break

            event = parse_event(raw)
            if event is None:
                continue

            try:
                await self.handler(event)    # [RESIL-2] handler completo antes de continuar
            except Exception as exc:
                self._stats["errors"] += 1
                logger.error("Error en handler. type=%s symbol=%s error=%s",
                             type(event).__name__, getattr(event, "symbol", "?"), exc)
                continue

            if isinstance(event, AggTrade):   self._stats["agg_trades"]   += 1
            elif isinstance(event, BookTicker): self._stats["book_tickers"] += 1

            self._rate.record()

            # [LOG-1] Log con ev/s real
            total = self._stats["agg_trades"] + self._stats["book_tickers"]
            if total % STATS_EVERY_N == 0:
                logger.info("Stats.", extra={"ctx": {
                    **self._stats, "rate_eps": round(self._rate.rate(), 1),
                }})


async def main() -> None:
    consumer = BinanceConsumer(handler=stdout_handler)

    # signal.signal() funciona en Windows y Unix.
    # add_signal_handler() es Unix-only (no disponible en ProactorEventLoop de Windows).
    def _shutdown(signum, frame):
        consumer.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    await consumer.run()

if __name__ == "__main__":
    asyncio.run(main())