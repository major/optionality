"""Sync stock splits data from Polygon API to Delta Lake."""

from datetime import date

import polars as pl

from optionality.polygon_client import PolygonClient
from optionality.validators import validate_split_data
from optionality.loaders.flatfile_loader import get_earliest_flatfile_date
from optionality.storage.delta_manager import delta
from optionality.config import get_settings
from optionality.logger import logger


def sync_all_splits(
    polygon_client: PolygonClient,
) -> int:
    """
    Sync ALL splits using efficient bulk API query.

    Strategy:
    1. Find earliest date from stock flatfiles
    2. Download ALL splits from Polygon (bulk query - cheap!)
    3. Overwrite existing splits table
    4. Return count of splits synced

    This ensures splits data is always fresh and uses only 1 API call.

    Args:
        polygon_client: PolygonClient instance

    Returns:
        Total number of splits synced
    """
    settings = get_settings()

    # Get earliest stock flatfile date
    stocks_path = settings.flatfiles_path / "stocks"
    earliest_date = get_earliest_flatfile_date(stocks_path)

    if earliest_date is None:
        logger.warning("⚠️ No stock flatfiles found, cannot determine date range")
        return 0

    logger.info(f"🔄 Syncing ALL splits from {earliest_date} to today (bulk API call)...")

    # Fetch ALL splits using bulk API (no ticker parameter!)
    splits = polygon_client.get_all_splits(earliest_date, date.today())

    if not splits:
        logger.info("ℹ️ No splits found in date range")
        return 0

    logger.info(f"📦 Downloaded {len(splits)} splits from Polygon")

    # Validate and prepare data (deduplicate by API's unique id)
    logger.info("💾 Preparing splits data...")
    warnings_count = 0
    splits_dict = {}  # Key: split_id, Value: split data

    for split in splits:
        # Validate split data
        errors = validate_split_data(split["split_from"], split["split_to"])
        if errors:
            logger.warning(
                f"  ⚠️ Validation warning for {split['ticker']}: {', '.join(errors)}"
            )
            warnings_count += 1

        # Use dict to automatically deduplicate by Polygon's unique split ID
        split_id = split["id"]
        splits_dict[split_id] = {
            "id": split_id,
            "ticker": split["ticker"],
            "execution_date": split["execution_date"],
            "split_from": float(split["split_from"]),
            "split_to": float(split["split_to"]),
            "split_factor": float(split["split_factor"]),
        }

    duplicates_removed = len(splits) - len(splits_dict)
    if duplicates_removed > 0:
        logger.warning(f"⚠️ Removed {duplicates_removed} duplicate splits from API response")

    # Create DataFrame from splits
    splits_df = pl.DataFrame(list(splits_dict.values()))

    # Write to Delta table (overwrite mode)
    logger.info(f"⚡ Writing {len(splits_df)} splits to Delta Lake...")
    delta.write_splits(splits_df, mode="overwrite")

    if warnings_count > 0:
        logger.warning(f"⚠️ {warnings_count} validation warnings")

    logger.success(f"✅ Synced {len(splits_df)} splits total")

    return len(splits_df)


def sync_splits_for_ticker(
    polygon_client: PolygonClient,
    ticker: str,
) -> int:
    """
    Sync splits for a specific ticker.

    Args:
        polygon_client: PolygonClient instance
        ticker: Stock ticker symbol

    Returns:
        Number of splits synced for this ticker
    """
    settings = get_settings()

    # Get earliest stock flatfile date
    stocks_path = settings.flatfiles_path / "stocks"
    earliest_date = get_earliest_flatfile_date(stocks_path)

    if earliest_date is None:
        logger.warning("⚠️ No stock flatfiles found, cannot determine date range")
        return 0

    logger.info(f"🔄 Syncing splits for {ticker}...")

    # Fetch splits for this ticker
    splits = polygon_client.get_all_splits(earliest_date, date.today())

    # Filter for this ticker
    ticker_splits = [s for s in splits if s["ticker"] == ticker]

    if not ticker_splits:
        logger.info(f"ℹ️ No splits found for {ticker}")
        return 0

    logger.info(f"📦 Downloaded {len(ticker_splits)} splits for {ticker}")

    # Prepare DataFrame
    splits_data = []
    for split in ticker_splits:
        splits_data.append({
            "id": split["id"],
            "ticker": split["ticker"],
            "execution_date": split["execution_date"],
            "split_from": float(split["split_from"]),
            "split_to": float(split["split_to"]),
            "split_factor": float(split["split_factor"]),
        })

    splits_df = pl.DataFrame(splits_data)

    # Read existing splits, filter out this ticker, then append new data
    if delta._table_exists(delta.splits_path):
        existing_splits = delta.scan_splits().filter(pl.col("ticker") != ticker).collect()
        combined_df = pl.concat([existing_splits, splits_df])
    else:
        combined_df = splits_df

    # Write back (overwrite mode)
    delta.write_splits(combined_df, mode="overwrite")

    logger.success(f"✅ Synced {len(splits_df)} splits for {ticker}")

    return len(splits_df)
