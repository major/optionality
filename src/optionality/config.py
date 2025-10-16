# config.py
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Polygon API
    polygon_api_key: str = ""

    # Polygon S3 Flatfiles (for source data)
    polygon_flatfiles_access_key: str = ""
    polygon_flatfiles_secret_key: str = ""
    polygon_flatfiles_endpoint: str = "https://files.polygon.io"

    # Data source configuration (for flatfiles)
    data_source: str = "s3"  # "local" or "s3"

    # S3 bucket and prefix settings (for Polygon flatfiles)
    stocks_s3_bucket: str = "flatfiles"
    stocks_s3_prefix: str = "us_stocks_sip/day_aggs_v1"
    options_s3_bucket: str = "flatfiles"
    options_s3_prefix: str = "us_options_opra/day_aggs_v1"

    # File paths (for local data source)
    flatfiles_path: Path = Path("./flatfiles")

    # Storage settings (Delta Lake) 🎯
    storage_backend: str = "s3"  # "s3" or "local" - S3 is default! ☁️
    storage_path: str = "s3://major-optionality/delta"  # S3 URI or local path

    # AWS S3 Settings (for Delta Lake storage) 🔐
    # Note: In GitHub Actions, AWS credentials come from OIDC, not env vars
    aws_region: str = "us-east-1"
    aws_access_key_id: str = ""  # Optional - leave empty for OIDC/IAM roles
    aws_secret_access_key: str = ""  # Optional - leave empty for OIDC/IAM roles
    aws_session_token: str = ""  # Optional - used by GitHub Actions OIDC

    # Performance settings
    batch_size: int = 10000
    max_concurrent_downloads: int = 2  # Simultaneous downloads from Polygon
    max_concurrent_file_processing: int = 2  # Simultaneous CSV file processing

    # Compression settings
    zstd_compression_level: int = 9  # 1 (fastest) to 22 (slowest, best compression)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def is_s3_storage(self) -> bool:
        """Check if storage backend is S3."""
        return self.storage_backend == "s3" or self.storage_path.startswith("s3://")

    def get_storage_options(self) -> dict[str, str]:
        """
        Get storage options for Delta Lake S3 access.

        Returns AWS credentials if configured, otherwise returns empty dict
        (to use default credential chain from environment/IAM role).
        """
        if not self.is_s3_storage():
            return {}

        # Build storage options for S3
        storage_opts: dict[str, str] = {
            "region_name": self.aws_region,
        }

        # Add credentials if explicitly configured
        # (GitHub Actions OIDC sets AWS_* env vars automatically)
        if self.aws_access_key_id:
            storage_opts["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            storage_opts["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            storage_opts["aws_session_token"] = self.aws_session_token

        return storage_opts


def get_settings() -> Settings:
    return Settings()
