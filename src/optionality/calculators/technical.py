"""Calculate technical indicators using polars-talib."""

import polars as pl
import polars_talib as plta

from optionality.storage.delta_manager import delta
from optionality.logger import logger


def calculate_technical_indicators() -> int:
    """
    Calculate technical indicators for all stock data.

    Calculates the following indicators:
    - SMA 20, 50, 200: Simple Moving Averages of closing price
    - Volume SMA 20: Simple Moving Average of volume
    - ATR: Average True Range (14-day)

    Indicators are calculated per-ticker using historical adjusted prices.

    Returns:
        Number of rows written to stocks_technical table
    """
    logger.info("📊 Calculating technical indicators...")

    # Load ALL adjusted stock data (lazy scan)
    logger.info("  📖 Scanning adjusted stock data...")
    adjusted_df = (
        delta.scan_stocks_adjusted()
        .sort("ticker", "window_start")  # Critical for time-series calculations
        .collect()
    )

    if len(adjusted_df) == 0:
        logger.warning("  ⚠️ No adjusted data found")
        return 0

    logger.info(f"  🧮 Calculating indicators for {len(adjusted_df):,} rows...")

    # Calculate technical indicators using polars-talib
    # Group by ticker to ensure calculations respect ticker boundaries
    technical_df = (
        adjusted_df
        .group_by("ticker", maintain_order=True)
        .agg([
            pl.col("window_start"),
            pl.col("close"),
            pl.col("high"),
            pl.col("low"),
            pl.col("volume"),
        ])
        # Explode to get back to row-per-date format
        .explode(["window_start", "close", "high", "low", "volume"])
        # Calculate indicators within each group
        .with_columns([
            # Simple Moving Averages of close price
            pl.col("close").ta.sma(timeperiod=20).over("ticker").alias("sma_20"),
            pl.col("close").ta.sma(timeperiod=50).over("ticker").alias("sma_50"),
            pl.col("close").ta.sma(timeperiod=200).over("ticker").alias("sma_200"),
            # Simple Moving Average of volume
            pl.col("volume").cast(pl.Float64).ta.sma(timeperiod=20).over("ticker").alias("volume_sma_20"),
            # Average True Range (requires high, low, close)
            pl.col("close").ta.atr(
                high=pl.col("high"),
                low=pl.col("low"),
                timeperiod=14
            ).over("ticker").alias("atr"),
        ])
        # Select only the columns we need for the technical table
        .select([
            "ticker",
            "window_start",
            "sma_20",
            "sma_50",
            "sma_200",
            "volume_sma_20",
            "atr",
        ])
    )

    logger.info(f"  💾 Writing {len(technical_df):,} technical indicator rows...")

    # Overwrite entire technical table (like we do with adjusted)
    delta.write_stocks_technical(technical_df, mode="overwrite")

    logger.success(f"✅ Calculated technical indicators for {len(technical_df):,} rows")

    return len(technical_df)
