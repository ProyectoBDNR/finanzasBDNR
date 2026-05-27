"""
tests/test_amihud.py
---------------------
Tests para feature_engine.amihud.add_amihud_illiq.

Ejecutar: python -m pytest tests/test_amihud.py -v
"""

import pytest
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

from feature_engine.amihud import add_amihud_illiq


# ---------------------------------------------------------------------------
# SparkSession compartida
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("cryptoflow-tests-amihud")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Schema y helpers
# ---------------------------------------------------------------------------

SCHEMA = StructType([
    StructField("symbol",        StringType(),    False),
    StructField("window_start",  TimestampType(), False),
    StructField("log_return",    DoubleType(),    True),
    StructField("price_qty_sum", DoubleType(),    True),
])

T0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _rows(symbol: str, n: int, log_return: float, price_qty_sum: float):
    return [
        (symbol, T0 + timedelta(minutes=i), log_return, price_qty_sum)
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAmihudIlliq:

    def test_constant_inputs_give_expected_value(self, spark):
        """
        Con log_return = 0.001 y price_qty_sum = 1e6, el ratio instantáneo
        es 0.001 / 1e6 = 1e-9. Escalado por 1e10 da 10. El promedio rolling
        de un valor constante es el mismo valor → amihud_illiq esperado = 10.
        """
        rows = _rows("BTCUSDT", 30, 0.001, 1e6)
        df = spark.createDataFrame(rows, SCHEMA)

        result = add_amihud_illiq(df, periods=10).orderBy("window_start").collect()

        # Toda fila (incluso las primeras, donde la ventana es parcial pero
        # el valor instantáneo es constante) debe dar 10.0
        for row in result:
            assert row["amihud_illiq"] == pytest.approx(10.0, rel=1e-9)

    def test_partial_window_uses_available_data(self, spark):
        """
        Spark calcula el promedio rolling con los datos disponibles cuando
        la ventana está incompleta — no devuelve NaN. Verificamos esto
        explícitamente para documentar el comportamiento.
        """
        rows = _rows("ETHUSDT", 5, 0.002, 2e6)
        df = spark.createDataFrame(rows, SCHEMA)

        result = (
            add_amihud_illiq(df, periods=10)
            .orderBy("window_start")
            .collect()
        )

        # Ratio instantáneo: 0.002 / 2e6 = 1e-9 → escalado a 10
        # Como todas las filas tienen el mismo valor instantáneo, el promedio
        # parcial (sobre k filas, k < periods) también es 10.
        assert len(result) == 5
        for row in result:
            assert row["amihud_illiq"] is not None
            assert row["amihud_illiq"] == pytest.approx(10.0, rel=1e-9)

    def test_zero_volume_does_not_break(self, spark):
        """
        Una ventana con price_qty_sum = 0 no debe producir Inf/NaN. El
        ratio instantáneo se marca como null y se excluye del promedio.
        """
        rows = [
            ("BNBUSDT", T0 + timedelta(minutes=0), 0.001, 1e6),
            ("BNBUSDT", T0 + timedelta(minutes=1), 0.001, 0.0),   # zero volume
            ("BNBUSDT", T0 + timedelta(minutes=2), 0.001, 1e6),
        ]
        df = spark.createDataFrame(rows, SCHEMA)

        result = (
            add_amihud_illiq(df, periods=3)
            .orderBy("window_start")
            .collect()
        )

        # Fila 0: solo 1 dato válido (10.0) → media = 10
        # Fila 1: 1 dato válido (la fila 0) + null → media = 10
        # Fila 2: 2 datos válidos (filas 0 y 2) → media = 10
        for row in result:
            assert row["amihud_illiq"] == pytest.approx(10.0, rel=1e-9)

    def test_partitions_by_symbol(self, spark):
        """
        El rolling no debe cruzar el límite entre símbolos. BTC con valor
        constante 10 y ETH con valor constante 20 no se mezclan.
        """
        rows_btc = _rows("BTCUSDT", 5, 0.001, 1e6)    # → 10
        rows_eth = _rows("ETHUSDT", 5, 0.002, 1e6)    # → 0.002/1e6*1e10 = 20
        df = spark.createDataFrame(rows_btc + rows_eth, SCHEMA)

        result = add_amihud_illiq(df, periods=10).collect()

        by_sym = {}
        for row in result:
            by_sym.setdefault(row["symbol"], []).append(row["amihud_illiq"])

        for v in by_sym["BTCUSDT"]:
            assert v == pytest.approx(10.0, rel=1e-9)
        for v in by_sym["ETHUSDT"]:
            assert v == pytest.approx(20.0, rel=1e-9)
