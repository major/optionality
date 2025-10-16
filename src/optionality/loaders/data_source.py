"""Abstract data source layer for local and S3 file access."""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import polars as pl
from fsspec import AbstractFileSystem

from optionality.config import Settings
from optionality.logger import logger
from optionality.loaders.flatfile_loader import (
    discover_flatfiles,
    get_file_date_from_path,
)
from optionality.loaders.s3_filesystem import (
    build_s3_path,
    get_storage_options,
    list_available_dates,
    list_available_years,
)
from optionality.loaders.boundary_detector import get_available_date_range


class DataSource(ABC):
    """Abstract base class for data sources (local or S3)."""

    def __init__(self, settings: Settings, data_type: str):
        """
        Initialize data source.

        Args:
            settings: Application settings
            data_type: Either "stocks" or "options"
        """
        self.settings = settings
        self.data_type = data_type

    @abstractmethod
    def discover_available_dates(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[date]:
        """
        Discover all available dates in the data source.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of available dates
        """
        pass

    @abstractmethod
    def scan_csv_lazy(self, target_date: date) -> pl.LazyFrame:
        """
        Create a lazy scan of CSV.GZ data for a specific date.

        Args:
            target_date: Date to scan

        Returns:
            Polars LazyFrame (not yet executed)
        """
        pass

    @abstractmethod
    def read_csv_gz(self, target_date: date) -> pl.DataFrame:
        """
        Read CSV.GZ data for a specific date into memory.

        Args:
            target_date: Date to read

        Returns:
            Polars DataFrame with data
        """
        pass

    @abstractmethod
    def get_date_range(self) -> Tuple[Optional[date], Optional[date]]:
        """
        Get the available date range for this data source.

        Returns:
            Tuple of (earliest_date, latest_date)
        """
        pass


class LocalDataSource(DataSource):
    """Data source implementation for local flatfiles."""

    def __init__(self, settings: Settings, data_type: str):
        """
        Initialize local data source.

        Args:
            settings: Application settings
            data_type: Either "stocks" or "options"
        """
        super().__init__(settings, data_type)
        self.base_path = settings.flatfiles_path / data_type
        logger.info(f"📁 Using local data source: {self.base_path}")

    def discover_available_dates(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[date]:
        """
        Discover all available dates from local flatfiles.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of available dates

        Examples:
            >>> ds = LocalDataSource(settings, "stocks")
            >>> dates = ds.discover_available_dates()
        """
        files = discover_flatfiles(self.base_path)

        dates = []
        for file_path in files:
            date_str = get_file_date_from_path(file_path)
            if date_str:
                try:
                    file_date = date.fromisoformat(date_str)

                    # Apply filters
                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue

                    dates.append(file_date)
                except ValueError:
                    continue

        return sorted(dates)

    def scan_csv_lazy(self, target_date: date) -> pl.LazyFrame:
        """
        Create a lazy scan of local CSV.GZ file.

        Args:
            target_date: Date to scan

        Returns:
            Polars LazyFrame

        Examples:
            >>> ds = LocalDataSource(settings, "stocks")
            >>> lazy_df = ds.scan_csv_lazy(date(2025, 1, 15))
            >>> df = lazy_df.collect()
        """
        # Build local file path: flatfiles/stocks/YYYY/MM/YYYY-MM-DD.csv.gz
        year = target_date.year
        month = target_date.month
        date_str = target_date.isoformat()
        file_path = self.base_path / str(year) / f"{month:02d}" / f"{date_str}.csv.gz"

        logger.debug(f"📖 Lazy scanning local file: {file_path}")
        return pl.scan_csv(file_path, has_header=True)

    def read_csv_gz(self, target_date: date) -> pl.DataFrame:
        """
        Read local CSV.GZ file into memory.

        Args:
            target_date: Date to read

        Returns:
            Polars DataFrame

        Examples:
            >>> ds = LocalDataSource(settings, "stocks")
            >>> df = ds.read_csv_gz(date(2025, 1, 15))
        """
        # Build local file path
        year = target_date.year
        month = target_date.month
        date_str = target_date.isoformat()
        file_path = self.base_path / str(year) / f"{month:02d}" / f"{date_str}.csv.gz"

        logger.debug(f"📖 Reading local file: {file_path}")
        return pl.read_csv(file_path, has_header=True)

    def get_date_range(self) -> Tuple[Optional[date], Optional[date]]:
        """
        Get the available date range from local files.

        Returns:
            Tuple of (earliest_date, latest_date)

        Examples:
            >>> ds = LocalDataSource(settings, "stocks")
            >>> earliest, latest = ds.get_date_range()
        """
        dates = self.discover_available_dates()
        if not dates:
            return None, None
        return min(dates), max(dates)


class S3DataSource(DataSource):
    """Data source implementation for S3 flatfiles using Polars direct scanning."""

    def __init__(self, settings: Settings, fs: AbstractFileSystem, data_type: str):
        """
        Initialize S3 data source.

        Args:
            settings: Application settings
            fs: fsspec S3 filesystem (for discovery only)
            data_type: Either "stocks" or "options"
        """
        super().__init__(settings, data_type)
        self.fs = fs
        self.storage_options = get_storage_options(settings)

        if data_type == "stocks":
            self.bucket = settings.stocks_s3_bucket
            self.prefix = settings.stocks_s3_prefix
        elif data_type == "options":
            self.bucket = settings.options_s3_bucket
            self.prefix = settings.options_s3_prefix
        else:
            raise ValueError(f"Invalid data_type: {data_type}")

        logger.info(f"☁️ Using S3 data source: s3://{self.bucket}/{self.prefix}")

    def _is_year_in_range(
        self, year: int, start_date: Optional[date], end_date: Optional[date]
    ) -> bool:
        """Check if year is within the date range."""
        if start_date and year < start_date.year:
            return False
        if end_date and year > end_date.year:
            return False
        return True

    def _filter_dates_in_range(
        self, dates: List[date], start_date: Optional[date], end_date: Optional[date]
    ) -> List[date]:
        """Filter dates to only include those within the specified range."""
        filtered = []
        for file_date in dates:
            if start_date and file_date < start_date:
                continue
            if end_date and file_date > end_date:
                continue
            filtered.append(file_date)
        return filtered

    def discover_available_dates(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[date]:
        """
        Discover all available dates from S3 using fsspec.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of available dates

        Examples:
            >>> fs = get_polygon_fs(settings)
            >>> ds = S3DataSource(settings, fs, "stocks")
            >>> dates = ds.discover_available_dates()
        """
        # Get all available years
        years = list_available_years(self.fs, self.bucket, self.prefix)

        all_dates = []
        for year in years:
            # Apply year filter
            if not self._is_year_in_range(year, start_date, end_date):
                continue

            # Get all dates for this year
            year_dates = list_available_dates(self.fs, self.bucket, self.prefix, year)

            # Apply date filters
            filtered_dates = self._filter_dates_in_range(year_dates, start_date, end_date)
            all_dates.extend(filtered_dates)

        return sorted(all_dates)

    def scan_csv_lazy(self, target_date: date) -> pl.LazyFrame:
        """
        Create a lazy scan of S3 CSV.GZ file using Polars.

        Polars scans directly from S3 - no downloading!

        Args:
            target_date: Date to scan

        Returns:
            Polars LazyFrame

        Examples:
            >>> fs = get_polygon_fs(settings)
            >>> ds = S3DataSource(settings, fs, "stocks")
            >>> lazy_df = ds.scan_csv_lazy(date(2025, 1, 15))
            >>> df = lazy_df.collect()
        """
        s3_path = build_s3_path(self.bucket, self.prefix, target_date)
        logger.debug(f"☁️ Lazy scanning S3: {s3_path}")

        return pl.scan_csv(s3_path, storage_options=self.storage_options)

    def read_csv_gz(self, target_date: date) -> pl.DataFrame:
        """
        Read S3 CSV.GZ file directly into memory using Polars.

        Polars reads directly from S3 - no downloading!

        Args:
            target_date: Date to read

        Returns:
            Polars DataFrame

        Examples:
            >>> fs = get_polygon_fs(settings)
            >>> ds = S3DataSource(settings, fs, "stocks")
            >>> df = ds.read_csv_gz(date(2025, 1, 15))
        """
        s3_path = build_s3_path(self.bucket, self.prefix, target_date)
        logger.debug(f"☁️ Reading from S3: {s3_path}")

        return pl.read_csv(s3_path, storage_options=self.storage_options, has_header=True)

    def get_date_range(self) -> Tuple[Optional[date], Optional[date]]:
        """
        Get the available date range from S3 using boundary detection.

        Returns:
            Tuple of (earliest_date, latest_date)

        Examples:
            >>> fs = get_polygon_fs(settings)
            >>> ds = S3DataSource(settings, fs, "stocks")
            >>> earliest, latest = ds.get_date_range()
        """
        return get_available_date_range(self.settings, self.fs, self.data_type)
