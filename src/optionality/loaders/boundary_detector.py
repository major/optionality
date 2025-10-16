"""Smart boundary detection for accessible data ranges in S3."""

from datetime import date, timedelta
from typing import Optional, Tuple
from functools import lru_cache

from fsspec import AbstractFileSystem
from tenacity import retry, stop_after_attempt, wait_exponential

from optionality.config import Settings
from optionality.logger import logger
from optionality.loaders.s3_filesystem import (
    build_s3_path,
    check_file_accessible,
    list_available_years,
)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def find_earliest_accessible_year(
    fs: AbstractFileSystem, bucket: str, prefix: str
) -> Optional[int]:
    """
    Find the earliest accessible year by working backwards from current year.

    Uses binary search approach to efficiently find the boundary.

    Args:
        fs: fsspec S3 filesystem
        bucket: S3 bucket name
        prefix: S3 prefix path

    Returns:
        Earliest accessible year, or None if no accessible data found

    Examples:
        >>> fs = get_polygon_fs(settings)
        >>> earliest = find_earliest_accessible_year(fs, "flatfiles", "us_stocks_sip/day_aggs_v1")
        2020
    """
    current_year = date.today().year

    # First, try to list years directly (fastest approach)
    available_years = list_available_years(fs, bucket, prefix)
    if available_years:
        logger.info(f"📅 Found years via directory listing: {available_years}")
        return min(available_years)

    # Fallback: Work backwards from current year
    logger.info(f"🔍 Searching for earliest accessible year, starting from {current_year}")

    for year in range(current_year, current_year - 10, -1):
        # Try first potential trading day of the year (Jan 3-5)
        for day in [3, 4, 5]:
            try:
                test_date = date(year, 1, day)
                s3_path = build_s3_path(bucket, prefix, test_date)

                if check_file_accessible(fs, s3_path):
                    logger.info(f"✅ Year {year} is accessible (found {s3_path})")
                    # Continue searching backwards
                    break
            except ValueError:
                continue
        else:
            # No accessible file found for this year - we've found the boundary
            logger.info(f"🚫 Year {year} is not accessible - boundary found")
            # Return the previous year (which was accessible)
            if year < current_year:
                return year + 1
            return None

    # Searched back 10 years without finding boundary
    return current_year - 9


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def find_latest_accessible_date(
    fs: AbstractFileSystem, bucket: str, prefix: str
) -> Optional[date]:
    """
    Find the latest accessible date in S3.

    Starts from today and works backwards.

    Args:
        fs: fsspec S3 filesystem
        bucket: S3 bucket name
        prefix: S3 prefix path

    Returns:
        Latest accessible date, or None if no accessible data found

    Examples:
        >>> fs = get_polygon_fs(settings)
        >>> latest = find_latest_accessible_date(fs, "flatfiles", "us_stocks_sip/day_aggs_v1")
        date(2025, 10, 15)
    """
    current_date = date.today()

    logger.info(f"🔍 Searching for latest accessible date from {current_date}")

    # Search backwards up to 30 days
    for days_back in range(30):
        test_date = current_date - timedelta(days=days_back)
        s3_path = build_s3_path(bucket, prefix, test_date)

        if check_file_accessible(fs, s3_path):
            logger.info(f"✅ Latest accessible date found: {test_date}")
            return test_date

    logger.warning("⚠️ No accessible files found in the last 30 days")
    return None


@lru_cache(maxsize=4)
def get_available_date_range(
    settings: Settings, fs: AbstractFileSystem, data_type: str
) -> Tuple[Optional[date], Optional[date]]:
    """
    Get the full available date range for a data type.

    This function is cached to avoid repeated boundary detection calls.

    Args:
        settings: Application settings
        fs: fsspec S3 filesystem
        data_type: Either "stocks" or "options"

    Returns:
        Tuple of (earliest_date, latest_date), or (None, None) if no data accessible

    Examples:
        >>> settings = get_settings()
        >>> fs = get_polygon_fs(settings)
        >>> earliest, latest = get_available_date_range(settings, fs, "stocks")
        (date(2020, 10, 14), date(2025, 10, 15))
    """
    if data_type == "stocks":
        bucket = settings.stocks_s3_bucket
        prefix = settings.stocks_s3_prefix
    elif data_type == "options":
        bucket = settings.options_s3_bucket
        prefix = settings.options_s3_prefix
    else:
        raise ValueError(f"Invalid data_type: {data_type}. Must be 'stocks' or 'options'")

    logger.info(f"🔍 Detecting data boundaries for {data_type}")

    # Find earliest year
    earliest_year = find_earliest_accessible_year(fs, bucket, prefix)
    if not earliest_year:
        logger.error(f"❌ No accessible data found for {data_type}")
        return None, None

    # Find earliest date in earliest year
    earliest_date = None
    for month in range(1, 13):
        for day in range(1, 32):
            try:
                test_date = date(earliest_year, month, day)
                s3_path = build_s3_path(bucket, prefix, test_date)

                if check_file_accessible(fs, s3_path):
                    earliest_date = test_date
                    logger.info(f"📅 Earliest date for {data_type}: {earliest_date}")
                    break
            except ValueError:
                continue
        if earliest_date:
            break

    # Find latest date
    latest_date = find_latest_accessible_date(fs, bucket, prefix)
    if latest_date:
        logger.info(f"📅 Latest date for {data_type}: {latest_date}")

    logger.info(f"🎯 Data range for {data_type}: {earliest_date} to {latest_date}")
    return earliest_date, latest_date


def build_date_list(start_date: date, end_date: date) -> list[date]:
    """
    Build list of all dates between start and end (inclusive).

    Note: This includes all calendar days. For trading days only,
    filter using pandas_market_calendars after.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)

    Returns:
        List of dates

    Examples:
        >>> dates = build_date_list(date(2025, 1, 1), date(2025, 1, 5))
        [date(2025, 1, 1), date(2025, 1, 2), ..., date(2025, 1, 5)]
    """
    if start_date > end_date:
        return []

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates
