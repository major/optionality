"""Split adjustment calculator using Polars for high performance."""

from datetime import date, datetime

import polars as pl

from optionality.storage.delta_manager import delta
from optionality.logger import logger


def calculate_adjusted_prices(
    raw_stocks_df: pl.DataFrame,
    splits_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Calculate split-adjusted stock prices using Polars.

    Formula: adjusted_price = raw_price / cumulative_split_factor

    Where cumulative_split_factor is the product of all split factors
    for splits that occurred ON OR AFTER each date.

    Args:
        raw_stocks_df: DataFrame with raw stock prices
        splits_df: DataFrame with splits (None = load from Delta table)

    Returns:
        DataFrame with split-adjusted prices (same schema as raw)

    Example:
        >>> raw_df = pl.DataFrame({...})
        >>> adjusted_df = calculate_adjusted_prices(raw_df)
    """
    # Load splits if not provided
    if splits_df is None:
        splits_lf = delta.scan_splits()
        splits_df = splits_lf.collect()

    # If no splits data, return raw prices as-is
    if len(splits_df) == 0:
        logger.info("📊 No splits found - returning raw prices")
        return raw_stocks_df

    # Ensure window_start is a date for comparison
    stocks_with_date = raw_stocks_df.with_columns(
        pl.col("window_start").cast(pl.Date).alias("price_date")
    )

    # Join stocks with splits where split execution date >= price date
    # This gives us all splits that affect each price
    joined = stocks_with_date.join(
        splits_df.select(["ticker", "execution_date", "split_factor"]),
        on="ticker",
        how="left",
    ).filter(
        # Only keep splits that occurred ON OR AFTER the price date
        (pl.col("execution_date") >= pl.col("price_date")) | pl.col("execution_date").is_null()
    )

    # Calculate cumulative split factor for each ticker/date combination
    # Group by ticker and price date, then multiply all split factors
    adjusted = (
        joined.group_by(["ticker", "window_start", "price_date"])
        .agg([
            # Keep original price columns
            pl.col("volume").first(),
            pl.col("open").first(),
            pl.col("close").first(),
            pl.col("high").first(),
            pl.col("low").first(),
            pl.col("transactions").first(),
            # Calculate cumulative split factor (product of all splits)
            pl.col("split_factor").product().alias("cumulative_factor"),
        ])
        .with_columns([
            # Fill null cumulative factors with 1.0 (no splits)
            pl.col("cumulative_factor").fill_null(1.0),
        ])
        .with_columns([
            # Apply split adjustments to prices
            (pl.col("open") / pl.col("cumulative_factor")).alias("open"),
            (pl.col("close") / pl.col("cumulative_factor")).alias("close"),
            (pl.col("high") / pl.col("cumulative_factor")).alias("high"),
            (pl.col("low") / pl.col("cumulative_factor")).alias("low"),
            # Volume is NOT adjusted for splits
        ])
        .select([
            "ticker",
            "window_start",
            "volume",
            "open",
            "close",
            "high",
            "low",
            "transactions",
        ])
        .sort(["ticker", "window_start"])
    )

    return adjusted


def get_tickers_with_splits(
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """
    Get list of tickers that had splits within a date range.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        DataFrame with ticker, split_count, first_split_date, last_split_date
    """
    splits_lf = delta.scan_splits(
        start_date=start_date,
        end_date=end_date,
    )

    return (
        splits_lf
        .group_by("ticker")
        .agg([
            pl.len().alias("split_count"),
            pl.col("execution_date").min().alias("first_split_date"),
            pl.col("execution_date").max().alias("last_split_date"),
        ])
        .sort(pl.col("split_count").desc())
        .collect()
    )


def get_splits_for_ticker(
    ticker: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """
    Get all splits for a ticker within a date range.

    Args:
        ticker: Stock ticker symbol
        start_date: Start date (None = all)
        end_date: End date (None = today)

    Returns:
        DataFrame with split data

    Example:
        >>> splits = get_splits_for_ticker('AAPL')
    """
    splits_lf = delta.scan_splits(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )

    return splits_lf.sort("execution_date").collect()


def format_split_ratio(split_from: float, split_to: float) -> str:
    """
    Format split ratio for display.

    Args:
        split_from: Shares before split
        split_to: Shares after split

    Returns:
        Formatted string like "4:1" or "1:4"

    Examples:
        >>> format_split_ratio(1, 4)
        '1:4 (forward split)'

        >>> format_split_ratio(4, 1)
        '4:1 (reverse split)'
    """
    split_type = "forward split" if split_to > split_from else "reverse split"
    return f"{int(split_from)}:{int(split_to)} ({split_type})"
