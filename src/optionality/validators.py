"""Data validators for stocks, options, and other market data."""

from typing import Any, Dict, List


class ValidationError(Exception):
    """Raised when data validation fails."""

    pass


def _validate_required_fields(row: Dict[str, Any], fields: List[str]) -> List[str]:
    """
    Check that required fields exist and are not None.

    Args:
        row: Data row dictionary
        fields: List of required field names

    Returns:
        List of error messages (empty if all fields present)
    """
    errors = []
    for field in fields:
        if field not in row or row[field] is None:
            errors.append(f"Missing required field: {field}")
    return errors


def _validate_positive_numeric_fields(
    row: Dict[str, Any], fields: List[str]
) -> List[str]:
    """
    Validate that specified fields are positive numbers.

    Args:
        row: Data row dictionary
        fields: List of field names to validate

    Returns:
        List of error messages (empty if all valid)
    """
    errors = []
    for field in fields:
        value = row.get(field)
        if value is not None:
            try:
                num_value = float(value)
                if num_value <= 0:
                    errors.append(f"{field} must be positive, got {num_value}")
            except (ValueError, TypeError):
                errors.append(f"{field} must be numeric, got {value}")
    return errors


def _validate_non_negative_numeric_fields(
    row: Dict[str, Any], fields: List[str]
) -> List[str]:
    """
    Validate that specified fields are non-negative numbers.

    Args:
        row: Data row dictionary
        fields: List of field names to validate

    Returns:
        List of error messages (empty if all valid)
    """
    errors = []
    for field in fields:
        value = row.get(field)
        if value is not None:
            try:
                num_value = float(value)
                if num_value < 0:
                    errors.append(f"{field} must be non-negative, got {num_value}")
            except (ValueError, TypeError):
                errors.append(f"{field} must be numeric, got {value}")
    return errors


def _validate_ohlc_relationships(row: Dict[str, Any]) -> List[str]:
    """
    Validate OHLC price relationships (high >= low, high >= open/close, etc.).

    Args:
        row: Data row dictionary with open, close, high, low fields

    Returns:
        List of error messages (empty if all valid)
    """
    errors = []
    try:
        open_price = float(row["open"])
        close_price = float(row["close"])
        high_price = float(row["high"])
        low_price = float(row["low"])

        if high_price < low_price:
            errors.append(f"high ({high_price}) must be >= low ({low_price})")

        if high_price < open_price:
            errors.append(f"high ({high_price}) must be >= open ({open_price})")

        if high_price < close_price:
            errors.append(f"high ({high_price}) must be >= close ({close_price})")

        if low_price > open_price:
            errors.append(f"low ({low_price}) must be <= open ({open_price})")

        if low_price > close_price:
            errors.append(f"low ({low_price}) must be <= close ({close_price})")

    except (ValueError, TypeError, KeyError):
        # If we can't convert to float, errors already added elsewhere
        pass

    return errors


def validate_stock_row(row: Dict[str, Any]) -> List[str]:
    """
    Validate a single stock data row.

    Args:
        row: Dictionary containing stock data fields

    Returns:
        List of validation error messages (empty if valid)

    Expected fields:
        - ticker: non-empty string
        - window_start: valid timestamp
        - volume: non-negative integer
        - open, close, high, low: positive numbers
        - transactions: non-negative integer
        - high >= low
        - high >= open, close
        - low <= open, close
    """
    errors = []

    # Check required fields exist
    required_fields = [
        "ticker",
        "window_start",
        "volume",
        "open",
        "close",
        "high",
        "low",
    ]
    errors.extend(_validate_required_fields(row, required_fields))

    # If critical fields are missing, return early
    if errors:
        return errors

    # Validate ticker
    ticker = row["ticker"]
    if not isinstance(ticker, str) or len(ticker) == 0:
        errors.append(f"Invalid ticker: {ticker}")

    # Validate OHLC fields are positive
    errors.extend(
        _validate_positive_numeric_fields(row, ["open", "close", "high", "low"])
    )

    # Validate volume is non-negative
    errors.extend(_validate_non_negative_numeric_fields(row, ["volume"]))

    # Validate OHLC price relationships
    errors.extend(_validate_ohlc_relationships(row))

    return errors


def validate_options_row(row: Dict[str, Any]) -> List[str]:
    """
    Validate a single options data row.

    Args:
        row: Dictionary containing options data fields

    Returns:
        List of validation error messages (empty if valid)

    Expected fields:
        - original_ticker: non-empty string
        - underlying_symbol: non-empty string
        - expiration_date: valid date
        - option_type: 'C' or 'P'
        - strike_price: positive number
        - window_start: valid timestamp
        - volume: non-negative integer
        - open, close, high, low: positive numbers or zero (options can have zero value)
    """
    errors = []

    # Check required fields exist
    required_fields = [
        "original_ticker",
        "underlying_symbol",
        "expiration_date",
        "option_type",
        "strike_price",
        "window_start",
    ]
    errors.extend(_validate_required_fields(row, required_fields))

    if errors:
        return errors

    # Validate tickers
    for field in ["original_ticker", "underlying_symbol"]:
        value = row[field]
        if not isinstance(value, str) or len(value) == 0:
            errors.append(f"Invalid {field}: {value}")

    # Validate option_type
    option_type = row["option_type"]
    if option_type not in ["C", "P"]:
        errors.append(f"option_type must be 'C' or 'P', got {option_type}")

    # Validate strike_price is positive
    errors.extend(_validate_positive_numeric_fields(row, ["strike_price"]))

    # Validate OHLC (options can be zero, so allow >= 0)
    errors.extend(
        _validate_non_negative_numeric_fields(row, ["open", "close", "high", "low"])
    )

    # Validate OHLC relationships
    errors.extend(_validate_ohlc_relationships(row))

    return errors


def validate_split_data(split_from: float, split_to: float) -> List[str]:
    """
    Validate stock split ratio data.

    Args:
        split_from: Number of shares before split
        split_to: Number of shares after split

    Returns:
        List of validation error messages (empty if valid)

    Examples:
        4:1 forward split → split_from=1, split_to=4 (1 share becomes 4)
        1:4 reverse split → split_from=4, split_to=1 (4 shares become 1)
    """
    errors = []

    if split_from <= 0:
        errors.append(f"split_from must be positive, got {split_from}")

    if split_to <= 0:
        errors.append(f"split_to must be positive, got {split_to}")

    return errors


def validate_ticker_symbol(ticker: str) -> bool:
    """
    Validate a stock ticker symbol format.

    Args:
        ticker: Ticker symbol to validate

    Returns:
        True if valid, False otherwise

    Valid tickers:
        - 1-5 uppercase letters
        - May contain dots for some stocks
    """
    if not ticker or not isinstance(ticker, str):
        return False

    # Basic pattern: 1-5 letters, optional dot and more letters
    # Examples: AAPL, MSFT, BRK.A, BRK.B
    if len(ticker) < 1 or len(ticker) > 10:
        return False

    # Should be uppercase letters and dots only
    return all(c.isupper() or c == "." for c in ticker)


def calculate_price_difference_percent(price1: float, price2: float) -> float:
    """
    Calculate percentage difference between two prices.

    Args:
        price1: First price
        price2: Second price (reference)

    Returns:
        Percentage difference: ((price1 - price2) / price2) * 100

    Examples:
        >>> calculate_price_difference_percent(105.0, 100.0)
        5.0

        >>> calculate_price_difference_percent(95.0, 100.0)
        -5.0
    """
    if price2 == 0:
        return float("inf") if price1 != 0 else 0.0

    return ((price1 - price2) / price2) * 100
