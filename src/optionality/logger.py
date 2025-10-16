"""Centralized logging configuration using loguru.

This module provides a pre-configured logger instance that can be imported
and used throughout the application. It supports:

- 🎨 Beautiful console output with colors and emojis
- 📝 Automatic file logging with rotation
- ⚡ Performance timing decorators
- 🔍 Context-aware structured logging
- 🏷️ Custom log levels and filtering

Usage:
    from optionality.logger import logger

    logger.info("Processing data")
    logger.debug("Debug information", extra={"ticker": "AAPL"})
    logger.error("An error occurred", exc_info=True)

    # Use timing decorator
    @logger_timer("my_function")
    def my_function():
        pass
"""

import sys
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, TypeVar

from loguru import logger as loguru_logger

# Remove default handler
loguru_logger.remove()

# 🎨 Console handler with emoji-rich formatting
console_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

loguru_logger.add(
    sys.stderr,
    format=console_format,
    level="INFO",
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# 📝 File handler with rotation (10 MB per file, keep 5 files)
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

file_format = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{name}:{function}:{line} | "
    "{message}"
)

loguru_logger.add(
    logs_dir / "optionality.log",
    format=file_format,
    level="DEBUG",
    rotation="10 MB",
    retention=5,
    compression="zip",
    backtrace=True,
    diagnose=True,
    enqueue=True,  # Async file writing
)

# 🚨 Error file handler (errors only)
loguru_logger.add(
    logs_dir / "errors.log",
    format=file_format,
    level="ERROR",
    rotation="10 MB",
    retention=10,
    compression="zip",
    backtrace=True,
    diagnose=True,
    enqueue=True,
)

# Export the configured logger
logger = loguru_logger


# Type variable for decorated functions
F = TypeVar("F", bound=Callable[..., Any])


def logger_timer(operation_name: str) -> Callable[[F], F]:
    """Decorator to log execution time of a function.

    Args:
        operation_name: Human-readable name for the operation

    Example:
        @logger_timer("Loading stock data")
        def load_stocks():
            pass
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.debug(f"⏳ Starting: {operation_name}")
            start_time = perf_counter()

            try:
                result = func(*args, **kwargs)
                elapsed = perf_counter() - start_time
                logger.info(f"✅ Completed: {operation_name} in {elapsed:.2f}s")
                return result
            except Exception as e:
                elapsed = perf_counter() - start_time
                logger.error(
                    f"❌ Failed: {operation_name} after {elapsed:.2f}s - {e!s}",
                    exc_info=True,
                )
                raise

        return wrapper  # type: ignore

    return decorator


def set_log_level(level: str) -> None:
    """Change the log level for console output.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Example:
        set_log_level("DEBUG")
    """
    # Remove existing handlers and re-add with new level
    loguru_logger.remove()

    loguru_logger.add(
        sys.stderr,
        format=console_format,
        level=level.upper(),
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

    # Keep file handlers at their original levels
    loguru_logger.add(
        logs_dir / "optionality.log",
        format=file_format,
        level="DEBUG",
        rotation="10 MB",
        retention=5,
        compression="zip",
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )

    loguru_logger.add(
        logs_dir / "errors.log",
        format=file_format,
        level="ERROR",
        rotation="10 MB",
        retention=10,
        compression="zip",
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )

    logger.info(f"🎚️ Log level changed to {level.upper()}")


# Example usage and testing
if __name__ == "__main__":
    logger.debug("🐛 This is a debug message")
    logger.info("ℹ️ This is an info message")
    logger.success("✅ This is a success message")
    logger.warning("⚠️ This is a warning message")
    logger.error("❌ This is an error message")

    # Test with extra context
    logger.info("Processing ticker", ticker="AAPL", volume=1000000)

    # Test timing decorator
    @logger_timer("Sample operation")
    def slow_function() -> None:
        import time

        time.sleep(0.1)

    slow_function()

    # Test error logging
    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("💥 Caught an exception!")

    logger.info("🎉 Logger test completed!")
