"""Delta Lake manager for optimized Polars-based storage with Zstd compression."""

from pathlib import Path
from typing import Any, Optional
from datetime import date, datetime

import polars as pl
from deltalake import write_deltalake, DeltaTable
import boto3

from optionality.config import get_settings
from optionality.logger import logger


# Schema definitions with optimized data types
STOCKS_RAW_SCHEMA = {
    "ticker": pl.String,
    "window_start": pl.Datetime("ms"),
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "transactions": pl.UInt32,
}

STOCKS_ADJUSTED_SCHEMA = {
    "ticker": pl.String,
    "window_start": pl.Datetime("ms"),
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "transactions": pl.UInt32,
}

SPLITS_SCHEMA = {
    "id": pl.String,
    "ticker": pl.String,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "split_factor": pl.Float64,
}

OPTIONS_SCHEMA = {
    "ticker": pl.String,  # Full options ticker (O: prefix stripped)
    "underlying_symbol": pl.String,
    "expiration_date": pl.Date,
    "option_type": pl.String,
    "strike_price": pl.Float32,
    "window_start": pl.Datetime("ms"),
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "transactions": pl.UInt32,
}

TICKERS_SCHEMA = {
    "ticker": pl.String,
    "name": pl.String,
    "market": pl.String,
    "locale": pl.String,
    "primary_exchange": pl.String,
    "type": pl.String,
    "active": pl.Boolean,
    "currency_name": pl.String,
    "currency_symbol": pl.String,
    "cik": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "last_updated_utc": pl.Datetime("ms"),
    "delisted_utc": pl.Datetime("ms"),
}


class DeltaLakeManager:
    """
    Manages Delta Lake tables with Zstd compression and lazy reading.

    Features:
    - Optimized schemas (Float32, UInt64, etc.)
    - Zstd level 12 compression for maximum space savings
    - Lazy reading with filter pushdown
    - Partitioned tables for fast single-ticker queries
    - S3 support with OIDC/IAM authentication 🚀
    """

    def __init__(self, base_path: str | Path | None = None):
        """
        Initialize Delta Lake manager.

        Args:
            base_path: Base path for Delta tables (None = use config)
                      Can be local path or S3 URI (s3://bucket/path)
        """
        settings = get_settings()
        self.settings = settings

        # Handle both local paths and S3 URIs
        if base_path is None:
            self.base_path = settings.storage_path
        elif isinstance(base_path, Path):
            self.base_path = str(base_path)
        else:
            self.base_path = base_path

        self.compression_level = settings.zstd_compression_level
        self.is_s3 = self.base_path.startswith("s3://")
        self.storage_options = settings.get_storage_options()

        # Table paths - use string concatenation for S3 URIs
        if self.is_s3:
            # S3 paths - use forward slashes
            self.stocks_raw_path = f"{self.base_path}/stocks_raw"
            self.stocks_adjusted_path = f"{self.base_path}/stocks_adjusted"
            self.splits_path = f"{self.base_path}/splits"
            self.options_path = f"{self.base_path}/options"
            self.tickers_path = f"{self.base_path}/tickers"
        else:
            # Local paths - use pathlib
            base = Path(self.base_path)
            self.stocks_raw_path = str(base / "stocks_raw")
            self.stocks_adjusted_path = str(base / "stocks_adjusted")
            self.splits_path = str(base / "splits")
            self.options_path = str(base / "options")
            self.tickers_path = str(base / "tickers")

    def _write_with_compression(
        self,
        table_path: str,
        df: pl.DataFrame,
        mode: str,
        partition_by: list[str],
    ) -> None:
        """
        Write DataFrame to Delta table with Zstd compression.

        Args:
            table_path: Path to Delta table (local or S3 URI)
            df: DataFrame to write
            mode: Write mode
            partition_by: Partition columns
        """
        # Convert to Arrow table with Zstd compression via Polars
        # Polars will write with optimal compression settings
        arrow_table = df.to_arrow()

        # Write to Delta Lake with compression configuration
        write_deltalake(
            table_path,
            arrow_table,
            mode=mode,
            partition_by=partition_by,
            storage_options=self.storage_options,  # 🔐 Pass AWS credentials for S3
            configuration={
                "delta.dataSkippingNumIndexedCols": "32",
                "delta.enableChangeDataFeed": "false",
            },
        )

    def _table_exists(self, table_path: str) -> bool:
        """
        Check if a Delta table exists.

        Args:
            table_path: Path to Delta table (local or S3 URI)

        Returns:
            True if table exists, False otherwise
        """
        try:
            DeltaTable(table_path, storage_options=self.storage_options)
            return True
        except Exception:
            return False

    def initialize_tables(self) -> None:
        """
        Initialize all Delta Lake tables with proper schemas.

        Creates empty tables if they don't exist.
        """
        logger.info("🔧 Initializing Delta Lake tables...")

        tables = [
            (self.stocks_raw_path, STOCKS_RAW_SCHEMA, ["ticker"], "stocks_raw"),
            (self.stocks_adjusted_path, STOCKS_ADJUSTED_SCHEMA, ["ticker"], "stocks_adjusted"),
            (self.splits_path, SPLITS_SCHEMA, ["ticker"], "splits"),
            (self.options_path, OPTIONS_SCHEMA, ["underlying_symbol"], "options"),
            (self.tickers_path, TICKERS_SCHEMA, ["ticker"], "tickers"),
        ]

        for table_path, schema, partition_by, name in tables:
            if not self._table_exists(table_path):
                logger.info(f"  📦 Creating {name}...")
                # Create empty DataFrame with schema
                empty_df = pl.DataFrame(schema=schema)
                self._write_with_compression(table_path, empty_df, "overwrite", partition_by)
            else:
                logger.info(f"  ✅ {name} already exists")

        logger.success("✅ Delta Lake tables initialized!")

    def drop_all_tables(self) -> None:
        """
        Delete all Delta Lake tables. Use with caution!

        Works with both local and S3 storage.
        """
        logger.warning("🗑️ Dropping all Delta Lake tables...")

        tables = [
            (self.stocks_raw_path, "stocks_raw"),
            (self.stocks_adjusted_path, "stocks_adjusted"),
            (self.splits_path, "splits"),
            (self.options_path, "options"),
            (self.tickers_path, "tickers"),
        ]

        if self.is_s3:
            # S3 deletion using boto3
            import urllib.parse

            # Parse S3 URI
            parsed = urllib.parse.urlparse(self.base_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")

            s3_client = boto3.client("s3", region_name=self.settings.aws_region)

            for table_path, name in tables:
                if self._table_exists(table_path):
                    logger.info(f"  🗑️ Deleting {name} from S3...")

                    # Parse table S3 path
                    table_parsed = urllib.parse.urlparse(table_path)
                    table_prefix = table_parsed.path.lstrip("/")

                    # List and delete all objects with this prefix
                    paginator = s3_client.get_paginator("list_objects_v2")
                    pages = paginator.paginate(Bucket=bucket, Prefix=table_prefix)

                    for page in pages:
                        if "Contents" in page:
                            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                            if objects:
                                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})

                    logger.info(f"    ✅ Deleted {name}")
        else:
            # Local deletion using shutil
            import shutil

            for table_path, name in tables:
                path_obj = Path(table_path)
                if path_obj.exists():
                    logger.info(f"  🗑️ Deleting {name}...")
                    shutil.rmtree(path_obj)

        logger.success("✅ All Delta Lake tables dropped!")

    # ==================== LAZY READ METHODS ====================

    def scan_stocks_raw(
        self,
        ticker: str | None = None,
        start_date: date | datetime | None = None,
        end_date: date | datetime | None = None,
    ) -> pl.LazyFrame:
        """
        Lazy scan of raw stock data with optional filters.

        Args:
            ticker: Filter by ticker (enables partition pruning!)
            start_date: Filter by start date
            end_date: Filter by end date

        Returns:
            LazyFrame with filter pushdown enabled (🚀 optimized for S3!)
        """
        if not self._table_exists(self.stocks_raw_path):
            logger.warning(f"⚠️ Table does not exist: {self.stocks_raw_path}")
            return pl.LazyFrame(schema=STOCKS_RAW_SCHEMA)

        # Start with lazy scan of Delta table (S3-aware! 🎯)
        lf = pl.scan_delta(self.stocks_raw_path, storage_options=self.storage_options)

        # Apply filters (pushdown to Parquet!)
        if ticker:
            lf = lf.filter(pl.col("ticker") == ticker)
        if start_date:
            lf = lf.filter(pl.col("window_start") >= start_date)
        if end_date:
            lf = lf.filter(pl.col("window_start") <= end_date)

        return lf

    def scan_stocks_adjusted(
        self,
        ticker: str | None = None,
        start_date: date | datetime | None = None,
        end_date: date | datetime | None = None,
    ) -> pl.LazyFrame:
        """
        Lazy scan of split-adjusted stock data with optional filters.

        Args:
            ticker: Filter by ticker (enables partition pruning!)
            start_date: Filter by start date
            end_date: Filter by end date

        Returns:
            LazyFrame with filter pushdown enabled (🚀 optimized for S3!)
        """
        if not self._table_exists(self.stocks_adjusted_path):
            logger.warning(f"⚠️ Table does not exist: {self.stocks_adjusted_path}")
            return pl.LazyFrame(schema=STOCKS_ADJUSTED_SCHEMA)

        lf = pl.scan_delta(self.stocks_adjusted_path, storage_options=self.storage_options)

        if ticker:
            lf = lf.filter(pl.col("ticker") == ticker)
        if start_date:
            lf = lf.filter(pl.col("window_start") >= start_date)
        if end_date:
            lf = lf.filter(pl.col("window_start") <= end_date)

        return lf

    def scan_splits(
        self,
        ticker: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.LazyFrame:
        """
        Lazy scan of stock splits with optional filters.

        Args:
            ticker: Filter by ticker (enables partition pruning!)
            start_date: Filter by execution date start
            end_date: Filter by execution date end

        Returns:
            LazyFrame with filter pushdown enabled (🚀 optimized for S3!)
        """
        if not self._table_exists(self.splits_path):
            logger.warning(f"⚠️ Table does not exist: {self.splits_path}")
            return pl.LazyFrame(schema=SPLITS_SCHEMA)

        lf = pl.scan_delta(self.splits_path, storage_options=self.storage_options)

        if ticker:
            lf = lf.filter(pl.col("ticker") == ticker)
        if start_date:
            lf = lf.filter(pl.col("execution_date") >= start_date)
        if end_date:
            lf = lf.filter(pl.col("execution_date") <= end_date)

        return lf

    def scan_options(
        self,
        underlying_symbol: str | None = None,
        ticker: str | None = None,
        start_date: date | datetime | None = None,
        end_date: date | datetime | None = None,
    ) -> pl.LazyFrame:
        """
        Lazy scan of options data with optional filters.

        Args:
            underlying_symbol: Filter by underlying symbol (enables partition pruning!)
            ticker: Filter by full options ticker
            start_date: Filter by start date
            end_date: Filter by end date

        Returns:
            LazyFrame with filter pushdown enabled (🚀 optimized for S3!)
        """
        if not self._table_exists(self.options_path):
            logger.warning(f"⚠️ Table does not exist: {self.options_path}")
            return pl.LazyFrame(schema=OPTIONS_SCHEMA)

        lf = pl.scan_delta(self.options_path, storage_options=self.storage_options)

        if underlying_symbol:
            lf = lf.filter(pl.col("underlying_symbol") == underlying_symbol)
        if ticker:
            lf = lf.filter(pl.col("ticker") == ticker)
        if start_date:
            lf = lf.filter(pl.col("window_start") >= start_date)
        if end_date:
            lf = lf.filter(pl.col("window_start") <= end_date)

        return lf

    def scan_tickers(
        self,
        ticker: str | None = None,
        active_only: bool = False,
    ) -> pl.LazyFrame:
        """
        Lazy scan of ticker metadata with optional filters.

        Args:
            ticker: Filter by ticker
            active_only: Only return active tickers

        Returns:
            LazyFrame with filter pushdown enabled (🚀 optimized for S3!)
        """
        if not self._table_exists(self.tickers_path):
            logger.warning(f"⚠️ Table does not exist: {self.tickers_path}")
            return pl.LazyFrame(schema=TICKERS_SCHEMA)

        lf = pl.scan_delta(self.tickers_path, storage_options=self.storage_options)

        if ticker:
            lf = lf.filter(pl.col("ticker") == ticker)
        if active_only:
            lf = lf.filter(pl.col("active") == True)

        return lf

    # ==================== WRITE METHODS ====================

    def write_stocks_raw(
        self,
        df: pl.DataFrame,
        mode: str = "append",
    ) -> None:
        """
        Write raw stock data to Delta table.

        Args:
            df: DataFrame with stock data
            mode: Write mode ("append", "overwrite", "merge")
        """
        # Cast to schema and sort by ticker, then date (for time-series analysis)
        df = (
            df.select([pl.col(col).cast(dtype) for col, dtype in STOCKS_RAW_SCHEMA.items()])
            .sort("ticker", "window_start")
        )

        self._write_with_compression(self.stocks_raw_path, df, mode, ["ticker"])

        logger.info(f"✅ Wrote {len(df):,} rows to stocks_raw")

    def write_stocks_adjusted(
        self,
        df: pl.DataFrame,
        mode: str = "append",
    ) -> None:
        """
        Write split-adjusted stock data to Delta table.

        Args:
            df: DataFrame with adjusted stock data
            mode: Write mode ("append", "overwrite", "merge")
        """
        # Cast to schema and sort by ticker, then date (for time-series analysis)
        df = (
            df.select([pl.col(col).cast(dtype) for col, dtype in STOCKS_ADJUSTED_SCHEMA.items()])
            .sort("ticker", "window_start")
        )

        self._write_with_compression(self.stocks_adjusted_path, df, mode, ["ticker"])

        logger.info(f"✅ Wrote {len(df):,} rows to stocks_adjusted")

    def write_splits(
        self,
        df: pl.DataFrame,
        mode: str = "overwrite",
    ) -> None:
        """
        Write stock splits to Delta table.

        Args:
            df: DataFrame with split data
            mode: Write mode ("append", "overwrite", "merge")
        """
        # Cast to schema
        df = df.select([pl.col(col).cast(dtype) for col, dtype in SPLITS_SCHEMA.items()])

        self._write_with_compression(self.splits_path, df, mode, ["ticker"])

        logger.info(f"✅ Wrote {len(df):,} rows to splits")

    def write_options(
        self,
        df: pl.DataFrame,
        mode: str = "append",
    ) -> None:
        """
        Write options data to Delta table.

        Args:
            df: DataFrame with options data
            mode: Write mode ("append", "overwrite", "merge")
        """
        # Cast to schema and sort by underlying_symbol, then date (for time-series analysis)
        df = (
            df.select([pl.col(col).cast(dtype) for col, dtype in OPTIONS_SCHEMA.items()])
            .sort("underlying_symbol", "window_start")
        )

        self._write_with_compression(self.options_path, df, mode, ["underlying_symbol"])

        logger.info(f"✅ Wrote {len(df):,} rows to options")

    def write_tickers(
        self,
        df: pl.DataFrame,
        mode: str = "overwrite",
    ) -> None:
        """
        Write ticker metadata to Delta table.

        Args:
            df: DataFrame with ticker metadata
            mode: Write mode ("append", "overwrite", "merge")
        """
        # Cast to schema
        df = df.select([pl.col(col).cast(dtype) for col, dtype in TICKERS_SCHEMA.items()])

        self._write_with_compression(self.tickers_path, df, mode, ["ticker"])

        logger.info(f"✅ Wrote {len(df):,} rows to tickers")

    # ==================== STATISTICS ====================

    def _get_single_table_stats(
        self, table_path: str, date_column: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Get statistics for a single Delta table.

        Args:
            table_path: Path to the Delta table
            date_column: Name of the date column to get min/max dates (None if no dates)

        Returns:
            Dictionary with count and optional date range
        """
        if not self._table_exists(table_path):
            return {}

        lf = pl.scan_delta(table_path, storage_options=self.storage_options)
        count = lf.select(pl.len()).collect().item()

        if date_column and count > 0:
            min_date = lf.select(pl.col(date_column).min()).collect().item()
            max_date = lf.select(pl.col(date_column).max()).collect().item()
            return {"count": count, "min_date": min_date, "max_date": max_date}
        else:
            return {"count": count}

    def get_table_stats(self) -> dict[str, dict[str, Any]]:
        """
        Get statistics for all Delta tables.

        Returns:
            Dictionary with table stats (row count, date ranges, etc.)
        """
        # Define table configurations: (name, path, date_column)
        table_configs = [
            ("stocks_raw", self.stocks_raw_path, "window_start"),
            ("stocks_adjusted", self.stocks_adjusted_path, "window_start"),
            ("splits", self.splits_path, "execution_date"),
            ("options", self.options_path, "window_start"),
            ("tickers", self.tickers_path, None),  # No date column
        ]

        stats = {}
        for name, path, date_column in table_configs:
            table_stats = self._get_single_table_stats(path, date_column)
            if table_stats:  # Only add if table exists
                stats[name] = table_stats

        return stats


# Global instance
delta = DeltaLakeManager()
