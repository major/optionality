"""Main commands for Delta Lake-based optionality system."""

import sys
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import polars as pl

from optionality.storage.delta_manager import delta
from optionality.polygon_client import PolygonClient
from optionality.loaders.stock_loader import (
    load_all_stock_files,
    load_stock_files_sequential,
    recalculate_all_adjustments,
)
from optionality.calculators.technical import calculate_technical_indicators
from optionality.loaders.stock_loader_gap_check import check_and_fill_trading_day_gaps
from optionality.loaders.options_loader import (
    load_all_options_files,
    load_incremental_options_files,
)
from optionality.loaders.s3_filesystem import (
    get_polygon_fs,
    build_s3_path,
    check_file_accessible,
)
from optionality.loaders import create_data_source
from optionality.config import get_settings
from optionality.sync.splits import sync_all_splits
from optionality.sync.tickers import sync_tickers
from optionality.verify.spot_checker import run_spot_checks
from optionality.logger import logger


def cmd_init() -> None:
    """Initialize the Delta Lake tables."""
    try:
        delta.initialize_tables()
        logger.success("✅ Delta Lake tables initialized successfully!")
    except Exception as e:
        logger.error(f"❌ Failed to initialize tables: {e}")
        sys.exit(1)


def cmd_load() -> None:
    """Perform initial bulk load of all data."""
    try:
        # Load stocks
        logger.info("📊 Step 1: Loading stock data...")
        stock_stats = load_all_stock_files()
        logger.info(
            f"  📈 Loaded {stock_stats['raw_rows']:,} raw rows, "
            f"{stock_stats['adjusted_rows']:,} adjusted rows"
        )

        # Load options
        logger.info("📊 Step 2: Loading options data...")
        options_stats = load_all_options_files()
        logger.info(f"  📉 Loaded {options_stats['rows_inserted']:,} options rows")

        # Sync tickers
        logger.info("📊 Step 3: Syncing ticker metadata...")
        polygon_client = PolygonClient()
        ticker_count = sync_tickers(polygon_client)
        logger.info(f"  🏷️ Synced {ticker_count:,} tickers")

        # Sync splits
        logger.info("📊 Step 4: Syncing ALL stock splits...")
        splits_count = sync_all_splits(polygon_client)
        logger.info(f"  ✂️ Synced {splits_count:,} splits")

        # Calculate technical indicators
        logger.info("📊 Step 5: Calculating technical indicators...")
        technical_rows = calculate_technical_indicators()
        logger.info(f"  📈 Calculated technical indicators for {technical_rows:,} rows")

        # Check for gaps and backfill
        logger.info("📊 Step 6: Checking for missing trading days...")
        gap_stats = check_and_fill_trading_day_gaps()
        if gap_stats["dates_filled"] > 0:
            logger.info(f"  🔧 Backfilled {gap_stats['dates_filled']} missing trading days")
        else:
            logger.info("  ✅ No gaps found")

        # Show stats
        logger.info("📊 Delta Lake Statistics:")
        stats = delta.get_table_stats()
        for table, table_stats in stats.items():
            if "min_date" in table_stats and table_stats["min_date"]:
                logger.info(
                    f"  {table}: {table_stats['count']:,} rows "
                    f"({table_stats['min_date']} to {table_stats['max_date']})"
                )
            else:
                logger.info(f"  {table}: {table_stats['count']:,} rows")

        logger.success("✅ Bulk load complete!")

    except Exception as e:
        logger.error(f"❌ Bulk load failed: {e}")
        sys.exit(1)


def cmd_update() -> None:
    """Run incremental daily update."""
    try:
        settings = get_settings()
        polygon_client = PolygonClient()

        # PHASE 1: Load raw stock data (no adjustments yet)
        logger.info("📊 Step 1: Loading new stock data (raw only)...")
        data_source = create_data_source(settings, "stocks")

        # Discover all available dates from data source
        all_dates = data_source.discover_available_dates()

        if all_dates:
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

            if missing_date_strs:
                # Convert back to date objects and sort
                missing_dates = sorted([date.fromisoformat(d) for d in missing_date_strs])

                logger.info(f"📊 Found {len(missing_dates)} missing dates to load")
                logger.info(f"📅 Date range: {missing_dates[0]} to {missing_dates[-1]}")

                # Load raw data only (no adjustments)
                raw_stats = load_stock_files_sequential(data_source, missing_dates)
                logger.info(
                    f"  📈 Loaded {raw_stats['raw_rows']:,} raw rows from "
                    f"{raw_stats['files_processed']} files"
                )
            else:
                logger.info("✅ No new stock data to load")
                raw_stats = {"files_processed": 0, "raw_rows": 0, "errors": 0}
        else:
            logger.warning("⚠️ No stock data found in source")
            raw_stats = {"files_processed": 0, "raw_rows": 0, "errors": 0}

        # PHASE 2: Sync splits (now we have the date range from Delta)
        logger.info("📊 Step 2: Syncing ALL stock splits...")
        splits_count = sync_all_splits(polygon_client)
        logger.info(f"  ✂️ Synced {splits_count:,} splits")

        # PHASE 3: Recalculate adjustments (with fresh splits)
        logger.info("📊 Step 3: Recalculating adjustments with fresh splits...")
        adjusted_rows = recalculate_all_adjustments()
        logger.info(f"  🔄 Recalculated {adjusted_rows:,} adjusted rows")

        # PHASE 4: Calculate technical indicators
        logger.info("📊 Step 4: Calculating technical indicators...")
        technical_rows = calculate_technical_indicators()
        logger.info(f"  📈 Calculated technical indicators for {technical_rows:,} rows")

        # PHASE 5: Check for gaps and backfill missing trading days
        logger.info("📊 Step 5: Checking for missing trading days...")
        gap_stats = check_and_fill_trading_day_gaps()
        if gap_stats["dates_filled"] > 0:
            logger.info(f"  🔧 Backfilled {gap_stats['dates_filled']} missing trading days")
        else:
            logger.info("  ✅ No gaps found")

        # Load incremental options data
        logger.info("📊 Step 6: Loading new options data...")
        options_stats = load_incremental_options_files()
        logger.info(
            f"  📉 Loaded {options_stats['rows_inserted']:,} options rows from "
            f"{options_stats['files_processed']} files"
        )

        # Sync tickers
        logger.info("📊 Step 7: Syncing ticker metadata...")
        ticker_count = sync_tickers(polygon_client)
        logger.info(f"  🏷️ Synced {ticker_count:,} tickers")

        # Run verification
        logger.info("📊 Step 8: Running spot checks...")
        run_spot_checks(polygon_client, num_tickers=5)

        # Show stats
        logger.info("📊 Delta Lake Statistics:")
        stats = delta.get_table_stats()
        for table, table_stats in stats.items():
            if "min_date" in table_stats and table_stats["min_date"]:
                logger.info(
                    f"  {table}: {table_stats['count']:,} rows "
                    f"({table_stats['min_date']} to {table_stats['max_date']})"
                )
            else:
                logger.info(f"  {table}: {table_stats['count']:,} rows")

        logger.success("✅ Incremental update complete!")

    except Exception as e:
        logger.error(f"❌ Update failed: {e}")
        sys.exit(1)


def cmd_verify() -> None:
    """Run spot-check verification."""
    try:
        polygon_client = PolygonClient()
        run_spot_checks(polygon_client, num_tickers=10, days_back=365)

        logger.success("✅ Verification complete!")

    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        sys.exit(1)


def cmd_stats() -> None:
    """Show Delta Lake table statistics."""
    try:
        logger.info("📊 Delta Lake Statistics")
        logger.info("=" * 60)

        stats = delta.get_table_stats()

        for table, table_stats in stats.items():
            logger.info(f"\n{table.upper()}:")
            logger.info(f"  Rows: {table_stats['count']:,}")

            if "min_date" in table_stats and table_stats["min_date"]:
                logger.info(f"  Date range: {table_stats['min_date']} to {table_stats['max_date']}")

    except Exception as e:
        logger.error(f"❌ Failed to get stats: {e}")
        sys.exit(1)


def cmd_clean() -> None:
    """Drop all Delta Lake tables and re-initialize (DESTRUCTIVE!)."""
    logger.warning("⚠️ WARNING: This will DELETE ALL DATA and re-initialize Delta Lake tables!")
    response = input("Type 'yes' to confirm: ")

    if response.lower() != "yes":
        logger.info("Aborted.")
        return

    try:
        delta.drop_all_tables()
        logger.success("✅ All Delta Lake tables dropped!")

        # Re-initialize the tables
        logger.info("🔄 Re-initializing Delta Lake tables...")
        delta.initialize_tables()
        logger.success("✅ Delta Lake tables cleaned and re-initialized!")

    except Exception as e:
        logger.error(f"❌ Failed to clean tables: {e}")
        sys.exit(1)


def cmd_check_files() -> None:
    """
    Check if new flatfiles are available in Polygon S3 that aren't in our Delta tables yet.

    This intelligently compares what's in our Delta tables vs what's available in Polygon's
    S3 bucket. It handles varying file drop times gracefully - files can appear as early as
    8PM ET on the day of the data, or as late as 11AM ET the next day.

    Exit codes:
        0: Both stocks AND options files are available for a new date (ready to update)
        2: No new files available yet (waiting for Polygon)
        1: Error occurred during check
    """
    try:
        from optionality.loaders.s3_filesystem import (
            list_available_dates,
        )
        import fsspec
        settings = get_settings()

        nyc_tz = ZoneInfo("America/New_York")
        now_nyc = datetime.now(nyc_tz)
        logger.info(f"🔍 Checking for new flatfiles...")
        logger.info(f"⏰ Current time in NYC: {now_nyc.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        # Step 1: Get latest dates from our Delta tables 📊
        stats = delta.get_table_stats()
        stocks_max_date = None
        options_max_date = None

        if "stocks_raw" in stats and "max_date" in stats["stocks_raw"]:
            # window_start is a datetime, extract the date
            max_datetime = stats["stocks_raw"]["max_date"]
            if max_datetime:
                stocks_max_date = max_datetime.date() if isinstance(max_datetime, datetime) else max_datetime
                logger.info(f"📈 Latest stocks data in Delta: {stocks_max_date}")

        if "options" in stats and "max_date" in stats["options"]:
            # window_start is a datetime, extract the date
            max_datetime = stats["options"]["max_date"]
            if max_datetime:
                options_max_date = max_datetime.date() if isinstance(max_datetime, datetime) else max_datetime
                logger.info(f"📉 Latest options data in Delta: {options_max_date}")

        # Step 2: Get filesystem for Polygon S3 🌐
        fs = fsspec.filesystem(
            "s3",
            key=settings.polygon_flatfiles_access_key,
            secret=settings.polygon_flatfiles_secret_key,
            client_kwargs={"endpoint_url": settings.polygon_flatfiles_endpoint},
        )

        # Step 3: Get available dates from Polygon S3 (check current year and last year) 📅
        current_year = now_nyc.year
        stocks_dates_set = set()
        options_dates_set = set()

        for year in [current_year - 1, current_year]:
            stocks_dates = list_available_dates(
                fs, settings.stocks_s3_bucket, settings.stocks_s3_prefix, year
            )
            options_dates = list_available_dates(
                fs, settings.options_s3_bucket, settings.options_s3_prefix, year
            )
            stocks_dates_set.update(stocks_dates)
            options_dates_set.update(options_dates)

        if not stocks_dates_set and not options_dates_set:
            logger.warning("⚠️ No files found in Polygon S3")
            logger.info("💡 Files typically arrive between 8PM ET (day of) and 11AM ET (next day)")
            sys.exit(2)

        # Step 4: Find NEW dates (in S3 but not in Delta) 🆕
        new_stocks_dates = {d for d in stocks_dates_set if stocks_max_date is None or d > stocks_max_date}
        new_options_dates = {d for d in options_dates_set if options_max_date is None or d > options_max_date}

        # Step 5: Find dates where BOTH stocks AND options are available 🎯
        dates_with_both = new_stocks_dates & new_options_dates

        if dates_with_both:
            # Get the earliest new date with both files
            earliest_new_date = min(dates_with_both)

            stocks_path = build_s3_path(
                settings.stocks_s3_bucket,
                settings.stocks_s3_prefix,
                earliest_new_date
            )
            options_path = build_s3_path(
                settings.options_s3_bucket,
                settings.options_s3_prefix,
                earliest_new_date
            )

            logger.success(f"🎉 New data available for {earliest_new_date}!")
            logger.info(f"  ✅ Stocks: {stocks_path}")
            logger.info(f"  ✅ Options: {options_path}")

            if len(dates_with_both) > 1:
                sorted_dates = [str(d) for d in sorted(dates_with_both)]
                logger.info(f"  📅 {len(dates_with_both)} total dates available: {', '.join(sorted_dates[:5])}...")

            sys.exit(0)

        # Step 6: Report what we're waiting for ⏳
        logger.info("⏳ No new data available yet (waiting for both stocks AND options)")

        if new_stocks_dates and not new_options_dates:
            sorted_dates = [str(d) for d in sorted(new_stocks_dates)]
            logger.info(f"  📈 Stocks available for: {', '.join(sorted_dates[:5])}")
            logger.info(f"  ⏳ Waiting for options data...")
        elif new_options_dates and not new_stocks_dates:
            sorted_dates = [str(d) for d in sorted(new_options_dates)]
            logger.info(f"  📉 Options available for: {', '.join(sorted_dates[:5])}")
            logger.info(f"  ⏳ Waiting for stocks data...")
        else:
            logger.info(f"  ⏳ Waiting for new files from Polygon...")

        logger.info("💡 Files typically arrive between 8PM ET (day of) and 11AM ET (next day)")
        sys.exit(2)  # Special exit code: not ready yet (not an error)

    except Exception as e:
        logger.error(f"❌ Error checking file availability: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Optionality: Stock and Options Data Warehouse (Delta Lake Edition)"
    )
    parser.add_argument(
        "command",
        choices=["init", "load", "update", "verify", "stats", "clean", "check-files"],
        help="Command to execute",
    )

    args = parser.parse_args()

    # Route to appropriate command
    if args.command == "init":
        cmd_init()
    elif args.command == "load":
        cmd_load()
    elif args.command == "update":
        cmd_update()
    elif args.command == "verify":
        cmd_verify()
    elif args.command == "stats":
        cmd_stats()
    elif args.command == "clean":
        cmd_clean()
    elif args.command == "check-files":
        cmd_check_files()
    else:
        parser.print_help()
        sys.exit(1)
