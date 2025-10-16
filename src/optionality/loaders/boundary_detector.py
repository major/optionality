"""Smart boundary detection for accessible data ranges in S3."""

from datetime import date, timedelta
from typing import Optional, Tuple

from fsspec import AbstractFileSystem
from tenacity import retry, stop_after_attempt, wait_exponential

from optionality.config import Settings
from optionality.loaders.s3_filesystem import (
    build_s3_path,
    check_file_accessible,
    list_available_years,
)
from optionality.logger import logger


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def find_earliest_accessible_year(
    fs: AbstractFileSystem, bucket: str, prefix: str
) -> Optional[int]:
    """
    Find the earliest accessible year by checking if files are actually accessible.

    Directory listing shows ALL years in the bucket, but we may not have access to older years.
    This function verifies accessibility by attempting to access actual files.

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

    # Get all years from directory listing
    available_years = list_available_years(fs, bucket, prefix)

    if available_years:
        # Check years from NEWEST to OLDEST (faster - recent data more likely accessible)
        # When we find the first inaccessible year, the previous year is our earliest
        earliest_accessible_year = None

        for year in sorted(available_years, reverse=True):
            # Try multiple dates throughout the year (access might start mid-year)
            # Check months from December backwards (faster)
            test_months = [12, 10, 7, 4, 1]

            year_is_accessible = False
            for month in test_months:
                # Try several days in each month (from end to beginning)
                for day in [25, 20, 15, 10, 5, 1]:
                    try:
                        test_date = date(year, month, day)
                        s3_path = build_s3_path(bucket, prefix, test_date)

                        if check_file_accessible(fs, s3_path):
                            # This year is accessible!
                            earliest_accessible_year = year
                            year_is_accessible = True
                            logger.debug(f"✅ Year {year} is accessible (found {s3_path})")
                            break
                    except ValueError:
                        continue

                if year_is_accessible:
                    break

            # If this year is NOT accessible and we found an accessible year before,
            # we've found the boundary!
            if not year_is_accessible and earliest_accessible_year is not None:
                logger.info(
                    f"✅ Earliest accessible year: {earliest_accessible_year} "
                    f"(year {year} not accessible)"
                )
                return earliest_accessible_year

        # If we get here, either all years are accessible or we found the oldest accessible year
        if earliest_accessible_year:
            logger.info(
                f"✅ Earliest accessible year: {earliest_accessible_year} "
                f"(all tested years back to {earliest_accessible_year} are accessible)"
            )
            return earliest_accessible_year

        # No accessible files found in any year
        logger.warning("⚠️ No accessible files found in any listed year")
        return None

    # Fallback: Work backwards from current year
    logger.info(
        f"🔍 Searching for earliest accessible year, starting from {current_year}"
    )

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


def _get_date_range_cached(
    fs: AbstractFileSystem, bucket: str, prefix: str, data_type: str
) -> Tuple[Optional[date], Optional[date]]:
    """
    Internal cached function for getting date range.

    Args:
        fs: fsspec S3 filesystem
        bucket: S3 bucket name
        prefix: S3 prefix path
        data_type: Either "stocks" or "options"

    Returns:
        Tuple of (earliest_date, latest_date), or (None, None) if no data accessible
    """
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


def get_available_date_range(
    settings: Settings, fs: AbstractFileSystem, data_type: str
) -> Tuple[Optional[date], Optional[date]]:
    """
    Get the full available date range for a data type.

    Note: Caching is handled internally using hashable keys.

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
        raise ValueError(
            f"Invalid data_type: {data_type}. Must be 'stocks' or 'options'"
        )

    # Use internal cached function with hashable parameters
    return _get_date_range_cached(fs, bucket, prefix, data_type)


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
