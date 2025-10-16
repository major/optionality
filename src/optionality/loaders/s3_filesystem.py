"""S3 filesystem utilities using fsspec for file discovery and boundary detection."""

from datetime import date
from typing import List, Optional

import fsspec
from fsspec import AbstractFileSystem

from optionality.config import Settings
from optionality.logger import logger


def get_polygon_fs(settings: Settings) -> AbstractFileSystem:
    """
    Get configured fsspec S3 filesystem for Polygon flatfiles.

    Args:
        settings: Application settings containing S3 credentials

    Returns:
        Configured fsspec S3 filesystem

    Examples:
        >>> settings = get_settings()
        >>> fs = get_polygon_fs(settings)
        >>> files = fs.ls('s3://flatfiles/us_stocks_sip/day_aggs_v1/2025/')
    """
    logger.info("🔧 Setting up fsspec S3 filesystem for Polygon flatfiles")

    fs = fsspec.filesystem(
        "s3",
        key=settings.polygon_flatfiles_access_key,
        secret=settings.polygon_flatfiles_secret_key,
        client_kwargs={"endpoint_url": settings.polygon_flatfiles_endpoint},
    )

    return fs


def build_s3_path(bucket: str, prefix: str, target_date: date) -> str:
    """
    Build S3 path for a specific date.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix path
        target_date: Date to build path for

    Returns:
        Full S3 path in format: s3://bucket/prefix/YYYY/MM/YYYY-MM-DD.csv.gz

    Examples:
        >>> build_s3_path("flatfiles", "us_stocks_sip/day_aggs_v1", date(2025, 1, 15))
        's3://flatfiles/us_stocks_sip/day_aggs_v1/2025/01/2025-01-15.csv.gz'
    """
    year = target_date.year
    month = target_date.month
    date_str = target_date.isoformat()

    return f"s3://{bucket}/{prefix}/{year}/{month:02d}/{date_str}.csv.gz"


def check_file_accessible(fs: AbstractFileSystem, s3_path: str) -> bool:
    """
    Check if a file is accessible in S3.

    This will return False for 403 (Forbidden) or 404 (Not Found) errors.

    Args:
        fs: fsspec S3 filesystem
        s3_path: Full S3 path to check

    Returns:
        True if file exists and is accessible, False otherwise

    Examples:
        >>> fs = get_polygon_fs(settings)
        >>> accessible = check_file_accessible(fs, 's3://flatfiles/us_stocks_sip/day_aggs_v1/2025/01/2025-01-15.csv.gz')
    """
    try:
        return fs.exists(s3_path)
    except Exception as e:
        logger.debug(f"❌ File not accessible: {s3_path} - {e}")
        return False


def list_available_years(fs: AbstractFileSystem, bucket: str, prefix: str) -> List[int]:
    """
    List all available years in S3 prefix.

    Args:
        fs: fsspec S3 filesystem
        bucket: S3 bucket name
        prefix: S3 prefix path

    Returns:
        List of years (as integers) found in the prefix

    Examples:
        >>> fs = get_polygon_fs(settings)
        >>> years = list_available_years(fs, "flatfiles", "us_stocks_sip/day_aggs_v1")
        [2020, 2021, 2022, 2023, 2024, 2025]
    """
    try:
        base_path = f"s3://{bucket}/{prefix}/"
        dirs = fs.ls(base_path)

        years = []
        for dir_path in dirs:
            # Extract year from path like 'flatfiles/us_stocks_sip/day_aggs_v1/2025'
            year_str = dir_path.split("/")[-1]
            if year_str.isdigit() and len(year_str) == 4:
                years.append(int(year_str))

        return sorted(years)

    except Exception as e:
        logger.warning(f"⚠️ Could not list years in {base_path}: {e}")
        return []


def list_available_dates(
    fs: AbstractFileSystem, bucket: str, prefix: str, year: int
) -> List[date]:
    """
    List all available dates for a specific year.

    Args:
        fs: fsspec S3 filesystem
        bucket: S3 bucket name
        prefix: S3 prefix path
        year: Year to list dates for

    Returns:
        List of dates found for the year

    Examples:
        >>> fs = get_polygon_fs(settings)
        >>> dates = list_available_dates(fs, "flatfiles", "us_stocks_sip/day_aggs_v1", 2025)
    """
    try:
        # List all month directories for the year
        year_path = f"s3://{bucket}/{prefix}/{year}/"
        month_dirs = fs.ls(year_path)

        dates = []
        for month_dir in month_dirs:
            # List all files in the month directory
            try:
                files = fs.ls(month_dir)
                for file_path in files:
                    # Extract date from filename like '2025-01-15.csv.gz'
                    filename = file_path.split("/")[-1]
                    if filename.endswith(".csv.gz"):
                        date_str = filename.replace(".csv.gz", "").replace(".csv", "")
                        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
                            try:
                                file_date = date.fromisoformat(date_str)
                                dates.append(file_date)
                            except ValueError:
                                continue
            except Exception as e:
                logger.debug(f"Could not list files in {month_dir}: {e}")
                continue

        return sorted(dates)

    except Exception as e:
        logger.warning(f"⚠️ Could not list dates for year {year}: {e}")
        return []


def get_storage_options(settings: Settings) -> dict:
    """
    Build storage_options dict for Polars S3 access.

    Args:
        settings: Application settings containing S3 credentials

    Returns:
        Dictionary with storage options for Polars

    Examples:
        >>> settings = get_settings()
        >>> storage_opts = get_storage_options(settings)
        >>> df = pl.read_csv(s3_path, storage_options=storage_opts)
    """
    return {
        "key": settings.polygon_flatfiles_access_key,
        "secret": settings.polygon_flatfiles_secret_key,
        "client_kwargs": {"endpoint_url": settings.polygon_flatfiles_endpoint},
    }
