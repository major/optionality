"""Gap detection and backfill for stock data."""

from datetime import date

import polars as pl
import pandas_market_calendars as mcal

from optionality.loaders import create_data_source
from optionality.loaders.stock_loader import load_stock_files_sequential, recalculate_all_adjustments
from optionality.storage.delta_manager import delta
from optionality.config import get_settings
from optionality.logger import logger


def check_and_fill_trading_day_gaps() -> dict:
    """
    Check for missing trading days in stocks_raw table and backfill from S3.

    Strategy:
    1. Get min/max dates from Delta stocks_raw table
    2. Use NYSE calendar to get all trading days in that range
    3. Find gaps (trading days not in Delta table)
    4. Load missing dates from S3
    5. Recalculate adjustments for consistency

    Returns:
        Dictionary with stats: gaps_found, dates_filled, raw_rows_added
    """
    settings = get_settings()

    # Check if stocks_raw table exists
    if not delta._table_exists(delta.stocks_raw_path):
        logger.info("ℹ️ No stocks_raw table found - skipping gap check")
        return {"gaps_found": 0, "dates_filled": 0, "raw_rows_added": 0}

    logger.info("🔍 Checking for missing trading days in stocks data...")

    # Get date range from Delta table
    stats = delta.get_table_stats()
    if "stocks_raw" not in stats or "min_date" not in stats["stocks_raw"]:
        logger.warning("⚠️ No stock data in Delta Lake yet - skipping gap check")
        return {"gaps_found": 0, "dates_filled": 0, "raw_rows_added": 0}

    min_datetime = stats["stocks_raw"]["min_date"]
    max_datetime = stats["stocks_raw"]["max_date"]

    if not min_datetime or not max_datetime:
        logger.warning("⚠️ No stock data in Delta Lake yet - skipping gap check")
        return {"gaps_found": 0, "dates_filled": 0, "raw_rows_added": 0}

    # Convert datetime to date
    min_date = min_datetime.date() if hasattr(min_datetime, 'date') else min_datetime
    max_date = max_datetime.date() if hasattr(max_datetime, 'date') else max_datetime

    logger.info(f"📅 Delta table date range: {min_date} to {max_date}")

    # Get all NYSE trading days in this range
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=str(min_date), end_date=str(max_date))
    trading_days_series = mcal.date_range(schedule, frequency='1D')

    # Convert pandas DatetimeIndex to Python date objects
    expected_trading_days = set(day.date() for day in trading_days_series)

    logger.info(f"📊 Expected NYSE trading days: {len(expected_trading_days)}")

    # Get all dates that exist in Delta table
    existing_df = (
        delta.scan_stocks_raw()
        .select(pl.col("window_start").cast(pl.Date).alias("date"))
        .unique()
        .collect()
    )
    existing_dates = set(existing_df["date"].to_list())

    logger.info(f"✅ Dates in Delta table: {len(existing_dates)}")

    # Find gaps (expected trading days not in Delta)
    missing_dates = expected_trading_days - existing_dates

    if not missing_dates:
        logger.success("✅ No gaps found - all trading days are present!")
        return {"gaps_found": 0, "dates_filled": 0, "raw_rows_added": 0}

    # Sort missing dates for better logging
    missing_dates_sorted = sorted(missing_dates)

    logger.warning(f"⚠️ Found {len(missing_dates)} missing trading days!")
    logger.info(f"📅 Gap range: {missing_dates_sorted[0]} to {missing_dates_sorted[-1]}")

    # Show first few missing dates
    if len(missing_dates_sorted) <= 10:
        logger.info(f"  Missing dates: {', '.join(str(d) for d in missing_dates_sorted)}")
    else:
        logger.info(f"  First 10 missing: {', '.join(str(d) for d in missing_dates_sorted[:10])}")
        logger.info(f"  Last 10 missing: {', '.join(str(d) for d in missing_dates_sorted[-10:])}")

    # Create data source and load missing dates
    logger.info("🔄 Backfilling missing trading days from S3...")
    data_source = create_data_source(settings, "stocks")

    # Load missing dates sequentially
    raw_stats = load_stock_files_sequential(data_source, missing_dates_sorted)

    logger.info(f"  📈 Loaded {raw_stats['raw_rows']:,} raw rows from {raw_stats['files_processed']} missing dates")

    # Recalculate all adjustments to ensure consistency
    if raw_stats['files_processed'] > 0:
        logger.info("🔄 Recalculating all adjustments after backfill...")
        recalculate_all_adjustments()

    logger.success(
        f"✅ Backfilled {raw_stats['files_processed']} missing trading days "
        f"({raw_stats['raw_rows']:,} rows)"
    )

    return {
        "gaps_found": len(missing_dates),
        "dates_filled": raw_stats['files_processed'],
        "raw_rows_added": raw_stats['raw_rows'],
    }
