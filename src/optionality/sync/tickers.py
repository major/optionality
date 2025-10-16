"""Sync ticker metadata from Polygon API to Delta Lake."""

from datetime import datetime

import polars as pl

from optionality.polygon_client import PolygonClient
from optionality.validators import validate_ticker_symbol
from optionality.storage.delta_manager import delta
from optionality.logger import logger


def sync_tickers(
    polygon_client: PolygonClient,
    market: str = "stocks",
    active: bool = True,
    limit: int = 1000,
) -> int:
    """
    Sync ALL ticker metadata using bulk API query.

    Strategy:
    1. Download ALL tickers from Polygon (bulk query - cheap!)
    2. Overwrite existing tickers table
    3. Return count of tickers synced

    This ensures ticker data is always fresh and uses only 1 API call.

    Args:
        polygon_client: PolygonClient instance
        market: Market type ('stocks', 'options', 'crypto')
        active: Only sync active tickers
        limit: Max number of tickers to fetch

    Returns:
        Number of tickers synced
    """
    logger.info(f"🔄 Syncing ALL {market} tickers from Polygon (bulk API call)...")

    # Fetch ALL tickers from Polygon
    tickers = polygon_client.get_all_tickers(market=market, active=active, limit=limit)

    if not tickers:
        logger.warning("⚠️ No tickers found")
        return 0

    logger.info(f"📦 Downloaded {len(tickers)} tickers from Polygon")

    # Prepare ticker data
    logger.info("💾 Preparing ticker data...")
    ticker_rows = []
    skipped_count = 0

    for ticker_data in tickers:
        try:
            # Validate ticker symbol
            if not validate_ticker_symbol(ticker_data["ticker"]):
                logger.warning(f"  ⚠️ Skipping invalid ticker: {ticker_data['ticker']}")
                skipped_count += 1
                continue

            # Convert timestamps if present
            last_updated_utc = None
            if ticker_data.get("last_updated_utc"):
                try:
                    last_updated_utc = datetime.fromisoformat(
                        ticker_data["last_updated_utc"].replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError):
                    pass

            delisted_utc = None
            if ticker_data.get("delisted_utc"):
                try:
                    delisted_utc = datetime.fromisoformat(
                        ticker_data["delisted_utc"].replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError):
                    pass

            # Add to list
            ticker_rows.append({
                "ticker": ticker_data["ticker"],
                "name": ticker_data.get("name"),
                "market": ticker_data.get("market"),
                "locale": ticker_data.get("locale"),
                "primary_exchange": ticker_data.get("primary_exchange"),
                "type": ticker_data.get("type"),
                "active": ticker_data.get("active"),
                "currency_name": ticker_data.get("currency_name"),
                "currency_symbol": ticker_data.get("currency_symbol"),
                "cik": ticker_data.get("cik"),
                "composite_figi": ticker_data.get("composite_figi"),
                "share_class_figi": ticker_data.get("share_class_figi"),
                "last_updated_utc": last_updated_utc,
                "delisted_utc": delisted_utc,
            })

        except Exception as e:
            logger.error(f"  ❌ Error preparing {ticker_data.get('ticker', 'unknown')}: {e}")
            skipped_count += 1

    # Create DataFrame
    tickers_df = pl.DataFrame(ticker_rows)

    # Write to Delta table (overwrite mode)
    logger.info(f"💾 Writing {len(tickers_df)} tickers to Delta Lake...")
    delta.write_tickers(tickers_df, mode="overwrite")

    logger.success(f"✅ Synced {len(tickers_df)} tickers ({skipped_count} skipped)")

    return len(tickers_df)
