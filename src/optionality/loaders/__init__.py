"""Data loaders for flatfiles."""

from optionality.config import Settings
from optionality.loaders.data_source import DataSource, LocalDataSource, S3DataSource
from optionality.loaders.s3_filesystem import get_polygon_fs
from optionality.logger import logger


def create_data_source(settings: Settings, data_type: str) -> DataSource:
    """
    Factory function to create appropriate data source based on settings.

    Args:
        settings: Application settings
        data_type: Either "stocks" or "options"

    Returns:
        DataSource instance (LocalDataSource or S3DataSource)

    Examples:
        >>> settings = get_settings()
        >>> stocks_ds = create_data_source(settings, "stocks")
        >>> options_ds = create_data_source(settings, "options")
    """
    if settings.data_source == "s3":
        logger.info(f"🚀 Creating S3 data source for {data_type}")
        fs = get_polygon_fs(settings)
        return S3DataSource(settings, fs, data_type)
    else:
        logger.info(f"📁 Creating local data source for {data_type}")
        return LocalDataSource(settings, data_type)


__all__ = [
    "create_data_source",
    "DataSource",
    "LocalDataSource",
    "S3DataSource",
]
