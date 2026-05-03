from dataclasses import dataclass, field
from datetime import datetime, timezone
import uuid


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _new_trace_id() -> str:
    return str(uuid.uuid4())


@dataclass
class AggTrade:
    """Parsed aggTrade event from Binance WebSocket."""

    # Binance fields
    symbol: str
    agg_trade_id: int
    price: float
    quantity: float
    trade_time: int        # ms epoch — tiempo real de ejecución del trade
    event_time: int        # ms epoch — tiempo de emisión del evento
    is_buyer_maker: bool

    # Trazabilidad
    trace_id: str = field(default_factory=_new_trace_id)
    ingestion_ts: datetime = field(default_factory=_now_utc)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "agg_trade_id": self.agg_trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "trade_time": self.trade_time,
            "event_time": self.event_time,
            "is_buyer_maker": self.is_buyer_maker,
            "trace_id": self.trace_id,
            "ingestion_ts": self.ingestion_ts.isoformat(),
        }


@dataclass
class BookTicker:
    """Parsed bookTicker event from Binance WebSocket."""

    # Binance fields
    symbol: str
    best_bid_price: float
    best_bid_qty: float
    best_ask_price: float
    best_ask_qty: float
    event_time: int        # ms epoch

    # Trazabilidad
    trace_id: str = field(default_factory=_new_trace_id)
    ingestion_ts: datetime = field(default_factory=_now_utc)

    @property
    def spread(self) -> float:
        return self.best_ask_price - self.best_bid_price

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "best_bid_price": self.best_bid_price,
            "best_bid_qty": self.best_bid_qty,
            "best_ask_price": self.best_ask_price,
            "best_ask_qty": self.best_ask_qty,
            "spread": self.spread,
            "event_time": self.event_time,
            "trace_id": self.trace_id,
            "ingestion_ts": self.ingestion_ts.isoformat(),
        }
