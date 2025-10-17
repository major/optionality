"""Stock data loader for Delta Lake with split adjustment calculation."""

from pathlib import Path
from datetime import date, datetime

import polars as pl
import pandas_market_calendars as mcal

from optionality.loaders.data_source import DataSource
from optionality.loaders import create_data_source
from optionality.splits.adjuster import calculate_adjusted_prices
from optionality.storage.delta_manager import delta, STOCKS_RAW_SCHEMA
from optionality.config import get_settings
from optionality.logger import logger


def load_stock_file(
    data_source: DataSource,
    target_date: date,
) -> tuple[int, int]:
    """
    Load a single stock data file into Delta Lake tables.

    Writes both raw and split-adjusted data.

    Args:
        data_source: DataSource instance (local or S3)
        target_date: Date to load

    Returns:
        Tuple of (raw_rows_inserted, adjusted_rows_inserted)
    """
    try:
        # Read CSV.GZ file using data source (handles local or S3!)
        df = data_source.read_csv_gz(target_date)

        if len(df) == 0:
            logger.warning(f"  ⚠️ {target_date}: Empty file")
            return 0, 0

        # Convert nanosecond timestamp to datetime (data transformation)
        df = df.with_columns(
            (pl.col("window_start") / 1_000_000).alias("window_start")
        )

        raw_count = len(df)

        # Write raw data (delta_manager handles schema casting)
        delta.write_stocks_raw(df, mode="append")

        # Calculate split-adjusted prices
        adjusted_df = calculate_adjusted_prices(df)

        # Write adjusted data
        delta.write_stocks_adjusted(adjusted_df, mode="append")

        adjusted_count = len(adjusted_df)

        return raw_count, adjusted_count

    except Exception as e:
        logger.error(f"❌ Error loading {target_date}: {e}")
        raise


def load_stock_files_sequential(
    data_source: DataSource,
    dates: list[date],
) -> dict:
    """
    Load multiple stock files sequentially to raw table only.

    Phase 1 of two-phase loading: loads raw data one file at a time.

    Args:
        data_source: DataSource instance (local or S3)
        dates: List of dates to load

    Returns:
        Dictionary with stats: files_processed, raw_rows, errors
    """
    logger.info(f"📦 Loading {len(dates)} dates sequentially...")

    total_raw_rows = 0
    errors = 0
    files_processed = 0

    for idx, target_date in enumerate(dates, 1):
        try:
            # Read CSV.GZ file using data source (handles local or S3!)
            df = data_source.read_csv_gz(target_date)

            if len(df) == 0:
                logger.warning(f"  ⚠️ {target_date}: Empty file")
                continue

            # Convert nanosecond timestamp to datetime (data transformation)
            df = df.with_columns(
                (pl.col("window_start") / 1_000_000).alias("window_start")
            )

            # Write raw data immediately
            delta.write_stocks_raw(df, mode="append")

            total_raw_rows += len(df)
            files_processed += 1

            # Log progress every 10 files or at the end
            if idx % 10 == 0 or idx == len(dates):
                logger.info(f"  📈 Progress: {idx}/{len(dates)} files ({files_processed} loaded, {total_raw_rows:,} rows)")

        except Exception as e:
            errors += 1
            logger.error(f"  ❌ {target_date}: {e}")

    logger.success(
        f"✅ Loaded {total_raw_rows:,} raw rows from {files_processed} files ({errors} errors)"
    )

    return {
        "files_processed": files_processed,
        "raw_rows": total_raw_rows,
        "errors": errors,
    }


def recalculate_all_adjustments() -> int:
    """
    Recalculate split adjustments for all stock data.

    Phase 2 of two-phase loading: reads all raw data, applies splits,
    and writes to adjusted table in a single efficient operation.

    Returns:
        Number of adjusted rows written
    """
    logger.info("🔄 Recalculating all adjustments from raw data...")

    # Load ALL raw data (lazy scan)
    logger.info("  📖 Scanning raw stock data...")
    raw_df = delta.scan_stocks_raw().collect()

    if len(raw_df) == 0:
        logger.warning("  ⚠️ No raw data found")
        return 0

    logger.info(f"  🧮 Calculating adjustments for {len(raw_df):,} rows...")

    # Calculate adjusted prices (loads splits once!)
    adjusted_df = calculate_adjusted_prices(raw_df)

    logger.info(f"  💾 Writing {len(adjusted_df):,} adjusted rows...")

    # Overwrite entire adjusted table
    delta.write_stocks_adjusted(adjusted_df, mode="overwrite")

    logger.success(f"✅ Recalculated {len(adjusted_df):,} adjusted rows")

    return len(adjusted_df)


def load_all_stock_files() -> dict:
    """
    Load all stock flatfiles into Delta Lake tables.

    Uses configured data source (local or S3).

    Returns:
        Dictionary with stats: files_processed, raw_rows, adjusted_rows, errors
    """
    settings = get_settings()
    data_source = create_data_source(settings, "stocks")

    logger.info(f"📦 Loading all stock data...")

    # Discover all available dates
    dates = data_source.discover_available_dates()

    if not dates:
        logger.warning("⚠️ No stock data found")
        return {"files_processed": 0, "raw_rows": 0, "adjusted_rows": 0, "errors": 0}

    logger.info(f"📊 Found {len(dates)} dates")

    # Process dates
    total_raw_rows = 0
    total_adjusted_rows = 0
    errors = 0

    for idx, target_date in enumerate(dates, 1):
        try:
            raw_count, adjusted_count = load_stock_file(data_source, target_date)
            total_raw_rows += raw_count
            total_adjusted_rows += adjusted_count

            # Log progress every 10 files or at the end
            if idx % 10 == 0 or idx == len(dates):
                logger.info(f"  📈 Progress: {idx}/{len(dates)} files ({total_raw_rows:,} raw rows, {total_adjusted_rows:,} adjusted rows)")

        except Exception as e:
            errors += 1
            logger.error(f"  ❌ {target_date}: {e}")

    logger.success(
        f"✅ Loaded {total_raw_rows:,} raw rows and {total_adjusted_rows:,} adjusted rows "
        f"from {len(dates)} dates ({errors} errors)"
    )

    return {
        "files_processed": len(dates),
        "raw_rows": total_raw_rows,
        "adjusted_rows": total_adjusted_rows,
        "errors": errors,
    }


def load_incremental_stock_files() -> dict:
    """
    Load stock files for any dates missing from Delta tables.

    Uses two-phase parallel loading:
    1. Load raw data from missing files in parallel
    2. Recalculate all adjustments in single pass

    Uses configured data source (local or S3).

    Returns:
        Dictionary with stats
    """
    settings = get_settings()
    data_source = create_data_source(settings, "stocks")

    logger.info("📦 Loading incremental stock data...")

    # Discover all available dates from data source
    all_dates = data_source.discover_available_dates()

    if not all_dates:
        logger.warning("⚠️ No stock data found")
        return {"files_processed": 0, "raw_rows": 0, "adjusted_rows": 0, "errors": 0}

    # Convert to set of date strings for comparison
    file_dates = set(d.isoformat() for d in all_dates)

    # Get all dates that exist in Delta table
    if delta._table_exists(delta.stocks_raw_path):
        existing_df = (
            delta.scan_stocks_raw()
            .select(pl.col("window_start").cast(pl.Date).alias("date"))
            .unique()
            .collect()
        )
        db_date_set = set(str(d) for d in existing_df["date"].to_list())
    else:
        db_date_set = set()

    # Find missing dates (in files but not in Delta table)
    missing_date_strs = file_dates - db_date_set

    if not missing_date_strs:
        logger.info("✅ No missing dates found - Delta tables are up to date!")
        return {"files_processed": 0, "raw_rows": 0, "adjusted_rows": 0, "errors": 0}

    # Convert back to date objects and sort
    missing_dates = sorted([date.fromisoformat(d) for d in missing_date_strs])

    logger.info(f"📊 Found {len(missing_dates)} missing dates to load")
    logger.info(f"📅 Date range: {missing_dates[0]} to {missing_dates[-1]}")

    # PHASE 1: Load raw data sequentially 📦
    logger.info("🔵 Phase 1: Loading raw data sequentially...")
    raw_stats = load_stock_files_sequential(data_source, missing_dates)

    # PHASE 2: Recalculate all adjustments 🧮
    logger.info("🟢 Phase 2: Recalculating adjustments...")
    adjusted_rows = recalculate_all_adjustments()

    logger.success(
        f"✅ Loaded {raw_stats['raw_rows']:,} raw rows and {adjusted_rows:,} adjusted rows "
        f"from {raw_stats['files_processed']} missing dates"
    )

    return {
        "files_processed": raw_stats['files_processed'],
        "raw_rows": raw_stats['raw_rows'],
        "adjusted_rows": adjusted_rows,
        "errors": raw_stats['errors'],
    }


def recalculate_adjustments_for_ticker(ticker: str) -> int:
    """
    Recalculate split adjustments for a specific ticker.

    Useful when splits data changes for a ticker.

    Args:
        ticker: Stock ticker symbol

    Returns:
        Number of rows updated
    """
    logger.info(f"🔄 Recalculating adjustments for {ticker}...")

    # Load raw data for this ticker
    raw_df = delta.scan_stocks_raw(ticker=ticker).collect()

    if len(raw_df) == 0:
        logger.warning(f"  ⚠️ No raw data found for {ticker}")
        return 0

    # Calculate adjusted prices
    adjusted_df = calculate_adjusted_prices(raw_df)

    # Overwrite adjusted data for this ticker (partition)
    delta.write_stocks_adjusted(adjusted_df, mode="overwrite")

    logger.success(f"✅ Recalculated {len(adjusted_df):,} rows for {ticker}")

    return len(adjusted_df)
