"""
enrichment/coingecko.py
------------------------
Cliente CoinGecko para metadata estática de activos.

Diseño:
  - Caché en memoria con TTL de 1 hora (la metadata cambia lentamente).
  - Mock integrado activable por variable de entorno o argumento:
    útil para tests y para ejecutar el job sin acceso a internet.
  - Retorna un dict plano listo para usarse en un join de Spark
    (se convierte a DataFrame con una fila por símbolo).
  - Rate limit de CoinGecko free tier: ~30 req/min. Con caché de 1h
    y 3 símbolos, se hacen máximo 3 requests por hora.

Mapeo símbolo Binance → ID CoinGecko:
  BTCUSDT → bitcoin
  ETHUSDT → ethereum
  BNBUSDT → binancecoin
"""

from __future__ import annotations

import os
import time
import logging
from typing import Optional

import requests

logger = logging.getLogger("enrichment.coingecko")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

COINGECKO_BASE   = "https://api.coingecko.com/api/v3"
CACHE_TTL_S      = 3600   # 1 hora
REQUEST_TIMEOUT  = 10     # segundos
USE_MOCK         = os.getenv("COINGECKO_MOCK", "false").lower() == "true"

SYMBOL_TO_ID = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "BNBUSDT": "binancecoin",
}

# ---------------------------------------------------------------------------
# Mock — datos representativos pero estáticos
# Usados en tests y cuando COINGECKO_MOCK=true
# ---------------------------------------------------------------------------

MOCK_METADATA: dict[str, dict] = {
    "BTCUSDT": {
        "symbol":               "BTCUSDT",
        "coingecko_id":         "bitcoin",
        "market_cap_usd":       1_300_000_000_000.0,
        "market_cap_rank":      1,
        "circulating_supply":   19_700_000.0,
        "total_supply":         21_000_000.0,
        "category":             "Layer 1",
        "description":          "Decentralized digital currency.",
    },
    "ETHUSDT": {
        "symbol":               "ETHUSDT",
        "coingecko_id":         "ethereum",
        "market_cap_usd":       430_000_000_000.0,
        "market_cap_rank":      2,
        "circulating_supply":   120_200_000.0,
        "total_supply":         None,
        "category":             "Smart Contract Platform",
        "description":          "Decentralized platform for smart contracts.",
    },
    "BNBUSDT": {
        "symbol":               "BNBUSDT",
        "coingecko_id":         "binancecoin",
        "market_cap_usd":       90_000_000_000.0,
        "market_cap_rank":      4,
        "circulating_supply":   153_000_000.0,
        "total_supply":         200_000_000.0,
        "category":             "Exchange Token",
        "description":          "Native token of the Binance ecosystem.",
    },
}


# ---------------------------------------------------------------------------
# Caché simple en memoria
# ---------------------------------------------------------------------------

class _Cache:
    def __init__(self, ttl_s: int):
        self._ttl = ttl_s
        self._store: dict[str, tuple[float, dict]] = {}   # key → (ts, value)

    def get(self, key: str) -> Optional[dict]:
        entry = self._store.get(key)
        if entry and (time.monotonic() - entry[0]) < self._ttl:
            return entry[1]
        return None

    def set(self, key: str, value: dict) -> None:
        self._store[key] = (time.monotonic(), value)

    def clear(self) -> None:
        self._store.clear()


_cache = _Cache(ttl_s=CACHE_TTL_S)


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------

def _fetch_from_api(coingecko_id: str, symbol: str) -> dict:
    """Hace la request real a CoinGecko y parsea el response."""
    url = f"{COINGECKO_BASE}/coins/{coingecko_id}"
    params = {
        "localization":   "false",
        "tickers":        "false",
        "market_data":    "true",
        "community_data": "false",
        "developer_data": "false",
    }

    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    market = data.get("market_data", {})
    categories = data.get("categories", [])

    return {
        "symbol":               symbol,
        "coingecko_id":         coingecko_id,
        "market_cap_usd":       float(market.get("market_cap", {}).get("usd") or 0),
        "market_cap_rank":      int(data.get("market_cap_rank") or 0),
        "circulating_supply":   float(market.get("circulating_supply") or 0),
        "total_supply":         float(market.get("total_supply") or 0) or None,
        "category":             categories[0] if categories else "Unknown",
        "description":          data.get("description", {}).get("en", "")[:200],
    }


def get_metadata(symbol: str, use_mock: bool = USE_MOCK) -> dict:
    """
    Retorna metadata de un activo por su símbolo Binance (ej. 'BTCUSDT').

    Args:
        symbol:   Símbolo en formato Binance (BTCUSDT, ETHUSDT, BNBUSDT)
        use_mock: Si True, retorna datos del mock sin hacer request HTTP

    Returns:
        dict con market_cap_usd, market_cap_rank, circulating_supply,
        total_supply, category, description.
    """
    symbol = symbol.upper()

    if use_mock:
        return MOCK_METADATA.get(symbol, _empty_metadata(symbol))

    cached = _cache.get(symbol)
    if cached:
        logger.debug("Cache hit para %s", symbol)
        return cached

    coingecko_id = SYMBOL_TO_ID.get(symbol)
    if not coingecko_id:
        logger.warning("Símbolo sin mapeo CoinGecko: %s", symbol)
        return _empty_metadata(symbol)

    try:
        metadata = _fetch_from_api(coingecko_id, symbol)
        _cache.set(symbol, metadata)
        logger.info("Metadata obtenida de CoinGecko para %s", symbol)
        return metadata
    except requests.RequestException as exc:
        logger.error("Error CoinGecko para %s: %s", symbol, exc)
        # Fallback al mock si la API falla
        return MOCK_METADATA.get(symbol, _empty_metadata(symbol))


def get_all_metadata(
    symbols: list[str] | None = None,
    use_mock: bool = USE_MOCK,
) -> list[dict]:
    """
    Retorna metadata de todos los símbolos monitoreados.
    Listo para convertir en un Spark DataFrame con una fila por símbolo.
    """
    if symbols is None:
        symbols = list(SYMBOL_TO_ID.keys())
    return [get_metadata(s, use_mock=use_mock) for s in symbols]


def _empty_metadata(symbol: str) -> dict:
    """Metadata vacía para símbolos no reconocidos."""
    return {
        "symbol":               symbol,
        "coingecko_id":         None,
        "market_cap_usd":       None,
        "market_cap_rank":      None,
        "circulating_supply":   None,
        "total_supply":         None,
        "category":             None,
        "description":          None,
    }