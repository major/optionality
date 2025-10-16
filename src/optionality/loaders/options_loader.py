"""Options data loader for Delta Lake with ticker parsing."""

from pathlib import Path
from datetime import date

import polars as pl
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from optionality.loaders.data_source import DataSource
from optionality.loaders import create_data_source
from optionality.parsers import parse_options_ticker
from optionality.storage.delta_manager import delta
from optionality.config import get_settings
from optionality.logger import logger


def load_options_file(
    data_source: DataSource,
    target_date: date,
) -> dict:
    """
    Load a single options data file into Delta Lake.

    Args:
        data_source: DataSource instance (local or S3)
        target_date: Date to load

    Returns:
        Dictionary with stats: rows_inserted, parse_errors
    """
    parse_errors = 0

    try:
        # Read CSV.GZ file using data source (handles local or S3!)
        df = data_source.read_csv_gz(target_date)

        if len(df) == 0:
            logger.warning(f"  ⚠️ {target_date}: Empty file")
            return {"rows_inserted": 0, "parse_errors": 0}

        # Convert nanosecond timestamp to datetime
        df = df.with_columns(
            (pl.col("window_start") / 1_000_000).cast(pl.Datetime("ms")).alias("window_start")
        )

        # Parse options tickers
        parsed_rows = []

        for row in df.iter_rows(named=True):
            original_ticker = row["ticker"]

            # Parse the ticker (handles "O:" prefix)
            parsed = parse_options_ticker(original_ticker)

            if not parsed:
                parse_errors += 1
                if parse_errors <= 5:  # Show first 5 errors
                    logger.warning(f"  ⚠️ Failed to parse ticker: {original_ticker}")
                continue

            # Strip "O:" prefix from ticker
            clean_ticker = original_ticker.replace("O:", "")

            parsed_rows.append({
                "ticker": clean_ticker,
                "underlying_symbol": parsed.underlying_symbol,
                "expiration_date": parsed.expiration_date,
                "option_type": parsed.option_type,
                "strike_price": float(parsed.strike_price),
                "window_start": row["window_start"],
                "volume": row.get("volume"),
                "open": row.get("open"),
                "close": row.get("close"),
                "high": row.get("high"),
                "low": row.get("low"),
                "transactions": row.get("transactions"),
            })

        if not parsed_rows:
            logger.warning(f"  ⚠️ {target_date}: No valid options tickers parsed")
            return {"rows_inserted": 0, "parse_errors": parse_errors}

        # Create DataFrame from parsed rows
        options_df = pl.DataFrame(parsed_rows)

        # Write to Delta table (delta_manager handles schema casting)
        delta.write_options(options_df, mode="append")

        return {
            "rows_inserted": len(options_df),
            "parse_errors": parse_errors,
        }

    except Exception as e:
        logger.error(f"❌ Error loading {target_date}: {e}")
        raise


def load_all_options_files() -> dict:
    """
    Load all options flatfiles into Delta Lake.

    Uses configured data source (local or S3).

    Returns:
        Dictionary with stats: files_processed, rows_inserted, parse_errors, file_errors
    """
    settings = get_settings()
    data_source = create_data_source(settings, "options")

    logger.info(f"📦 Loading all options data...")

    # Discover all available dates
    dates = data_source.discover_available_dates()

    if not dates:
        logger.warning("⚠️ No options data found")
        return {
            "files_processed": 0,
            "rows_inserted": 0,
            "parse_errors": 0,
            "file_errors": 0,
        }

    logger.info(f"📊 Found {len(dates)} dates")

    # Process dates with progress bar
    total_rows = 0
    total_parse_errors = 0
    file_errors = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Loading options files...", total=len(dates))

        for target_date in dates:
            try:
                result = load_options_file(data_source, target_date)
                total_rows += result["rows_inserted"]
                total_parse_errors += result["parse_errors"]

                logger.info(
                    f"  ✅ {target_date}: {result['rows_inserted']:,} rows "
                    f"({result['parse_errors']} parse errors)"
                )

            except Exception as e:
                file_errors += 1
                logger.error(f"  ❌ {target_date}: {e}")

            progress.update(task, advance=1)

    logger.success(f"✅ Loaded {total_rows:,} options rows from {len(dates)} dates")

    if total_parse_errors > 0:
        logger.warning(f"⚠️ {total_parse_errors:,} ticker parse errors")

    return {
        "files_processed": len(dates),
        "rows_inserted": total_rows,
        "parse_errors": total_parse_errors,
        "file_errors": file_errors,
    }


def load_incremental_options_files() -> dict:
    """
    Load options files for any dates missing from Delta tables.

    Uses configured data source (local or S3).

    Returns:
        Dictionary with stats
    """
    settings = get_settings()
    data_source = create_data_source(settings, "options")

    logger.info("📦 Loading incremental options data...")

    # Discover all available dates from data source
    all_dates = data_source.discover_available_dates()

    if not all_dates:
        logger.warning("⚠️ No options data found")
        return {
            "files_processed": 0,
            "rows_inserted": 0,
            "parse_errors": 0,
            "file_errors": 0,
        }

    # Convert to set of date strings for comparison
    file_dates = set(d.isoformat() for d in all_dates)

    # Get all dates that exist in Delta table
    if delta._table_exists(delta.options_path):
        existing_df = (
            delta.scan_options()
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
        return {
            "files_processed": 0,
            "rows_inserted": 0,
            "parse_errors": 0,
            "file_errors": 0,
        }

    # Convert back to date objects and sort
    missing_dates = sorted([date.fromisoformat(d) for d in missing_date_strs])

    logger.info(f"📊 Found {len(missing_dates)} missing dates to load")
    logger.info(f"📅 Date range: {missing_dates[0]} to {missing_dates[-1]}")

    # Load files
    total_rows = 0
    total_parse_errors = 0
    file_errors = 0

    for target_date in missing_dates:
        try:
            result = load_options_file(data_source, target_date)
            total_rows += result["rows_inserted"]
            total_parse_errors += result["parse_errors"]
            logger.info(f"  ✅ {target_date}: {result['rows_inserted']:,} rows")
        except Exception as e:
            file_errors += 1
            logger.error(f"  ❌ {target_date}: {e}")

    logger.success(f"✅ Loaded {total_rows:,} options rows from {len(missing_dates)} missing dates")

    return {
        "files_processed": len(missing_dates),
        "rows_inserted": total_rows,
        "parse_errors": total_parse_errors,
        "file_errors": file_errors,
    }
