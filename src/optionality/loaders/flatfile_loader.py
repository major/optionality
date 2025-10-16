"""Base flatfile loader with discovery and streaming capabilities."""

from pathlib import Path
from typing import List, Iterator, Optional
from datetime import date
import gzip

import polars as pl

from optionality.config import Settings
from optionality.logger import logger


def discover_flatfiles(
    base_path: Path,
    pattern: str = "**/*.csv.gz",
) -> List[Path]:
    """
    Discover all flatfiles matching a pattern.

    Args:
        base_path: Base directory to search in
        pattern: Glob pattern for files (default: **/*.csv.gz)

    Returns:
        List of Path objects sorted by modification time (oldest first)

    Examples:
        >>> discover_flatfiles(Path("./flatfiles/stocks"))
        [Path('./flatfiles/stocks/2020/10/2020-10-14.csv.gz'), ...]
    """
    if not base_path.exists():
        logger.warning(f"⚠️ Directory does not exist: {base_path}")
        return []

    files = sorted(base_path.glob(pattern))

    # Sort by modification time (oldest first)
    files.sort(key=lambda p: p.stat().st_mtime)

    return files


def stream_csv_gz(
    file_path: Path,
    batch_size: int = 10000,
) -> Iterator[pl.DataFrame]:
    """
    Stream a CSV.GZ file in batches using Polars lazy evaluation.

    Args:
        file_path: Path to the CSV.GZ file
        batch_size: Number of rows per batch

    Yields:
        Polars DataFrames containing batch_size rows

    Examples:
        >>> for batch in stream_csv_gz(Path("data.csv.gz")):
        ...     print(f"Processing {len(batch)} rows")
    """
    try:
        # Use Polars lazy reading for memory efficiency
        lazy_df = pl.scan_csv(
            file_path,
            has_header=True,
        )

        # Collect in batches
        offset = 0
        while True:
            batch = lazy_df.slice(offset, batch_size).collect()

            if len(batch) == 0:
                break

            yield batch
            offset += batch_size

    except Exception as e:
        logger.error(f"❌ Error reading {file_path}: {e}")
        raise


def read_csv_gz_full(file_path: Path) -> pl.DataFrame:
    """
    Read entire CSV.GZ file into memory using Polars.

    Use this for smaller files or when you need the full dataset.

    Args:
        file_path: Path to the CSV.GZ file

    Returns:
        Polars DataFrame with all data
    """
    return pl.read_csv(file_path, has_header=True)


def get_file_date_from_path(file_path: Path) -> str | None:
    """
    Extract date from flatfile path.

    Expected format: YYYY/MM/YYYY-MM-DD.csv.gz

    Args:
        file_path: Path to the flatfile

    Returns:
        Date string in YYYY-MM-DD format or None if not found

    Examples:
        >>> get_file_date_from_path(Path("2020/10/2020-10-14.csv.gz"))
        '2020-10-14'
    """
    # Extract filename without extension
    filename = file_path.stem.replace(".csv", "")

    # Check if it looks like a date (YYYY-MM-DD)
    if len(filename) == 10 and filename[4] == "-" and filename[7] == "-":
        return filename

    return None


def filter_new_files(
    all_files: List[Path],
    loaded_files: set[str],
) -> List[Path]:
    """
    Filter out files that have already been loaded.

    Args:
        all_files: List of all discovered files
        loaded_files: Set of file paths that have been loaded

    Returns:
        List of files that haven't been loaded yet
    """
    return [f for f in all_files if str(f) not in loaded_files]


def count_rows_in_gz(file_path: Path) -> int:
    """
    Count total rows in a gzipped CSV file.

    Args:
        file_path: Path to the CSV.GZ file

    Returns:
        Number of rows (excluding header)
    """
    with gzip.open(file_path, "rt") as f:
        # Skip header
        next(f)
        return sum(1 for _ in f)


def get_earliest_flatfile_date(base_path: Path, pattern: str = "**/*.csv.gz") -> Optional[date]:
    """
    Get the earliest date from flatfiles in a directory.

    Args:
        base_path: Base directory to search in
        pattern: Glob pattern for files (default: **/*.csv.gz)

    Returns:
        Earliest date found, or None if no valid dates found

    Examples:
        >>> get_earliest_flatfile_date(Path("./flatfiles/stocks"))
        date(2020, 10, 14)
    """
    files = discover_flatfiles(base_path, pattern)

    if not files:
        return None

    earliest_date = None

    for file_path in files:
        date_str = get_file_date_from_path(file_path)
        if date_str:
            try:
                file_date = date.fromisoformat(date_str)
                if earliest_date is None or file_date < earliest_date:
                    earliest_date = file_date
            except ValueError:
                continue

    return earliest_date


# S3 Utilities for Polars Direct Scanning


def build_storage_options(settings: Settings) -> dict:
    """
    Build storage_options dict for Polars S3 access.

    Args:
        settings: Application settings containing S3 credentials

    Returns:
        Dictionary with storage options for Polars

    Examples:
        >>> settings = get_settings()
        >>> storage_opts = build_storage_options(settings)
        >>> df = pl.read_csv(s3_path, storage_options=storage_opts)
    """
    return {
        "key": settings.polygon_flatfiles_access_key,
        "secret": settings.polygon_flatfiles_secret_key,
        "client_kwargs": {"endpoint_url": settings.polygon_flatfiles_endpoint},
    }


def scan_csv_gz_from_s3(s3_path: str, storage_options: dict) -> pl.LazyFrame:
    """
    Create lazy scan of S3 CSV.GZ file using Polars.

    Polars scans directly from S3 - no downloading!

    Args:
        s3_path: Full S3 path (e.g., s3://bucket/prefix/file.csv.gz)
        storage_options: Storage options dict from build_storage_options()

    Returns:
        Polars LazyFrame (not yet executed)

    Examples:
        >>> storage_opts = build_storage_options(settings)
        >>> lazy_df = scan_csv_gz_from_s3("s3://flatfiles/us_stocks_sip/day_aggs_v1/2025/01/2025-01-15.csv.gz", storage_opts)
        >>> df = lazy_df.collect()
    """
    logger.debug(f"☁️ Lazy scanning S3: {s3_path}")
    return pl.scan_csv(s3_path, storage_options=storage_options)


def read_csv_gz_from_s3(s3_path: str, storage_options: dict) -> pl.DataFrame:
    """
    Read S3 CSV.GZ file directly into memory using Polars.

    Polars reads directly from S3 - no downloading!

    Args:
        s3_path: Full S3 path (e.g., s3://bucket/prefix/file.csv.gz)
        storage_options: Storage options dict from build_storage_options()

    Returns:
        Polars DataFrame

    Examples:
        >>> storage_opts = build_storage_options(settings)
        >>> df = read_csv_gz_from_s3("s3://flatfiles/us_stocks_sip/day_aggs_v1/2025/01/2025-01-15.csv.gz", storage_opts)
    """
    logger.debug(f"☁️ Reading from S3: {s3_path}")
    return pl.read_csv(s3_path, storage_options=storage_options, has_header=True)
