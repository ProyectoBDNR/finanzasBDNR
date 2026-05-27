"""
processing/enrichment.py
-------------------------
Enriquecimiento de features cuantitativas con metadata estática por símbolo.

Cumple con la Etapa 4 del proyecto (Bases de Datos No Relacionales):
"se debe realizar el cruce de los datos del stream con datasets estáticos
para añadir contexto de negocio".

Dataset estático
────────────────
data/static/symbols_metadata.csv  — una fila por símbolo (BTC, ETH, BNB)
con atributos cualitativos no derivables del stream de mercado:
    full_name, category, sector, listing_date_binance,
    approx_market_cap_usd_billions, consensus_mechanism, is_exchange_token

Lógica de join
──────────────
La clave es `symbol` (cardinalidad muy baja: 3 valores). Se usa
`F.broadcast()` para forzar broadcast-hash join, evitando shuffle de
la tabla de features (que puede tener miles de ventanas por día).

El enriquecimiento es aditivo: agrega columnas nuevas y nunca modifica
las existentes. Si un símbolo no tiene match en la metadata, las nuevas
columnas quedan NULL (LEFT JOIN).
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, DateType,
)


# Ruta por defecto del CSV (relativa a la raíz del repo)
DEFAULT_METADATA_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "static" / "symbols_metadata.csv"
)

# Schema explícito — evita inferencia costosa y errores de tipo
_METADATA_SCHEMA = StructType([
    StructField("symbol",                            StringType(),  False),
    StructField("full_name",                         StringType(),  True),
    StructField("category",                          StringType(),  True),
    StructField("sector",                            StringType(),  True),
    StructField("listing_date_binance",              DateType(),    True),
    StructField("approx_market_cap_usd_billions",    DoubleType(),  True),
    StructField("consensus_mechanism",               StringType(),  True),
    StructField("is_exchange_token",                 BooleanType(), True),
])


def load_symbols_metadata(
    spark: SparkSession,
    path: str | os.PathLike | None = None,
) -> DataFrame:
    """
    Carga el CSV de metadata estática por símbolo como DataFrame de Spark.

    Args:
        spark: SparkSession activa.
        path:  Ruta al CSV. Si es None, usa DEFAULT_METADATA_PATH.

    Returns:
        DataFrame con schema fijo (ver _METADATA_SCHEMA).

    Raises:
        FileNotFoundError: si el CSV no existe en la ruta especificada.
    """
    csv_path = Path(path) if path is not None else DEFAULT_METADATA_PATH
    if not csv_path.exists():
        raise FileNotFoundError(f"Metadata CSV no encontrado: {csv_path}")

    return (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(_METADATA_SCHEMA)
        .csv(str(csv_path))
    )


def enrich_features(
    df_features: DataFrame,
    spark: SparkSession,
    metadata_path: str | os.PathLike | None = None,
) -> DataFrame:
    """
    Enriquece las features cuantitativas con metadata estática por símbolo.

    Estrategia:
        - LEFT JOIN sobre `symbol` (preserva todas las filas de features).
        - `F.broadcast()` sobre la metadata (3 filas) → broadcast-hash join,
          sin shuffle del lado grande.
        - Conserva intactas las columnas originales (solo agrega nuevas).

    Args:
        df_features:    DataFrame de salida de `final_feature_set()`.
        spark:          SparkSession activa.
        metadata_path:  Ruta opcional al CSV de metadata.

    Returns:
        df_features + columnas:
            full_name, category, sector, listing_date_binance,
            approx_market_cap_usd_billions, consensus_mechanism,
            is_exchange_token
    """
    df_meta = load_symbols_metadata(spark, metadata_path)

    return df_features.join(
        F.broadcast(df_meta),
        on="symbol",
        how="left",
    )
