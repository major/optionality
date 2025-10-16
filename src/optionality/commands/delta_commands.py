"""Main commands for Delta Lake-based optionality system."""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from optionality.storage.delta_manager import delta
from optionality.polygon_client import PolygonClient
from optionality.loaders.stock_loader import (
    load_all_stock_files,
    load_incremental_stock_files,
)
from optionality.loaders.options_loader import (
    load_all_options_files,
    load_incremental_options_files,
)
from optionality.loaders.s3_filesystem import (
    get_polygon_fs,
    build_s3_path,
    check_file_accessible,
)
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
        polygon_client = PolygonClient()

        # Sync ALL splits (fresh data every time!)
        logger.info("📊 Step 1: Syncing ALL stock splits...")
        splits_count = sync_all_splits(polygon_client)
        logger.info(f"  ✂️ Synced {splits_count:,} splits")

        # Load incremental stock data
        logger.info("📊 Step 2: Loading new stock data...")
        stock_stats = load_incremental_stock_files()
        logger.info(
            f"  📈 Loaded {stock_stats['raw_rows']:,} raw rows, "
            f"{stock_stats['adjusted_rows']:,} adjusted rows from "
            f"{stock_stats['files_processed']} files"
        )

        # Load incremental options data
        logger.info("📊 Step 3: Loading new options data...")
        options_stats = load_incremental_options_files()
        logger.info(
            f"  📉 Loaded {options_stats['rows_inserted']:,} options rows from "
            f"{options_stats['files_processed']} files"
        )

        # Run verification
        logger.info("📊 Step 4: Running spot checks...")
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
    Check if both stocks and options flatfiles are available for yesterday.

    Polygon drops files between 8PM NYC time on the day of data and 11AM NYC time
    the next day. This command checks if both files have arrived yet.

    Exit codes:
        0: Both files are available (ready to update)
        2: Files not yet available (waiting for Polygon)
        1: Error occurred during check
    """
    try:
        import fsspec
        settings = get_settings()

        # Get yesterday's date in NYC timezone
        nyc_tz = ZoneInfo("America/New_York")
        now_nyc = datetime.now(nyc_tz)
        yesterday = (now_nyc - timedelta(days=1)).date()

        logger.info(f"🔍 Checking if flatfiles are available for {yesterday}")
        logger.info(f"⏰ Current time in NYC: {now_nyc.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        # Get filesystem (using fsspec directly to avoid caching issues)
        fs = fsspec.filesystem(
            "s3",
            key=settings.polygon_flatfiles_access_key,
            secret=settings.polygon_flatfiles_secret_key,
            client_kwargs={"endpoint_url": settings.polygon_flatfiles_endpoint},
        )

        # Check stocks file
        stocks_path = build_s3_path(
            settings.stocks_s3_bucket,
            settings.stocks_s3_prefix,
            yesterday
        )
        stocks_available = check_file_accessible(fs, stocks_path)

        # Check options file
        options_path = build_s3_path(
            settings.options_s3_bucket,
            settings.options_s3_prefix,
            yesterday
        )
        options_available = check_file_accessible(fs, options_path)

        # Report results
        if stocks_available:
            logger.info(f"✅ Stocks file available: {stocks_path}")
        else:
            logger.info(f"⏳ Stocks file not yet available: {stocks_path}")

        if options_available:
            logger.info(f"✅ Options file available: {options_path}")
        else:
            logger.info(f"⏳ Options file not yet available: {options_path}")

        # Exit with appropriate code
        if stocks_available and options_available:
            logger.success(f"🎉 Both files are available for {yesterday}! Ready to update.")
            sys.exit(0)
        else:
            logger.info(f"⏳ Waiting for Polygon to drop files for {yesterday}...")
            logger.info("💡 Files typically arrive between 8PM ET (day of) and 11AM ET (next day)")
            sys.exit(2)  # Special exit code: not ready yet (not an error)

    except Exception as e:
        logger.error(f"❌ Error checking file availability: {e}")
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
