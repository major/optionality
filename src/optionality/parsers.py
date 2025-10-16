"""Parsers for options tickers and other data formats."""

import re
from datetime import datetime, date
from typing import Optional, NamedTuple


class OptionComponents(NamedTuple):
    """Parsed components of an options ticker."""

    underlying_symbol: str
    expiration_date: date
    option_type: str  # 'C' for Call, 'P' for Put
    strike_price: float


def parse_options_ticker(ticker: str) -> Optional[OptionComponents]:
    """
    Parse an options ticker in Polygon OCC format to extract components.

    Format: O:[UNDERLYING][YYMMDD][C/P][STRIKE]
    - Prefix: "O:" (2 characters)
    - UNDERLYING: Variable-length ticker symbol (1+ letters A-Z only, no digits)
    - YYMMDD: 6-digit expiration date
    - C/P: Single character option type (C=Call, P=Put)
    - STRIKE: 8-digit strike price in thousandths of a dollar

    Since the underlying symbol length is variable, we parse RIGHT TO LEFT:
    - Last 8 characters = strike price
    - 9th from right = option type (C/P)
    - Characters at positions -15 to -9 = date (YYMMDD)
    - Everything after "O:" prefix up to date = potential underlying (extract letters only)

    Args:
        ticker: The options ticker string to parse

    Returns:
        OptionComponents namedtuple if parsing succeeds, None otherwise

    Examples:
        >>> parse_options_ticker("O:AAPL210917C00145000")
        OptionComponents(underlying_symbol='AAPL', expiration_date=datetime.date(2021, 9, 17),
                        option_type='C', strike_price=145.0)

        >>> parse_options_ticker("O:ACB1260116C00001000")
        OptionComponents(underlying_symbol='ACB', expiration_date=datetime.date(2026, 1, 16),
                        option_type='C', strike_price=1.0)

        >>> parse_options_ticker("O:AQMS1251017P00002500")
        OptionComponents(underlying_symbol='AQMS', expiration_date=datetime.date(2025, 10, 17),
                        option_type='P', strike_price=2.5)
    """
    # Validate minimum length: O: (2) + underlying (1+) + date (6) + type (1) + strike (8) = 18+
    if not ticker or len(ticker) < 18:
        return None

    # Must start with "O:"
    if not ticker.startswith("O:"):
        return None

    # Remove "O:" prefix
    ticker_body = ticker[2:]

    # Validate minimum body length: underlying (1+) + date (6) + type (1) + strike (8) = 16+
    if len(ticker_body) < 16:
        return None

    try:
        # Parse from RIGHT TO LEFT (fixed-length fields)
        # Last 8 characters: strike price
        strike_str = ticker_body[-8:]

        # 9th character from right: option type (C or P)
        option_type = ticker_body[-9]

        # Characters at positions -15 to -9 (6 digits): expiration date
        date_str = ticker_body[-15:-9]

        # Everything before that: potential underlying symbol (may contain digits to discard)
        underlying_raw = ticker_body[:-15]

        # Extract only A-Z letters for the underlying symbol
        underlying = re.sub(r'[^A-Z]', '', underlying_raw.upper())

        # Validate components
        if not underlying:  # Must have at least 1 letter
            return None

        if option_type not in ('C', 'P'):
            return None

        if not strike_str.isdigit() or len(strike_str) != 8:
            return None

        if not date_str.isdigit() or len(date_str) != 6:
            return None

        # Parse date: YYMMDD
        year = int("20" + date_str[0:2])  # Assume 20xx century
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        expiration = date(year, month, day)

        # Parse strike price (divide by 1000)
        strike = int(strike_str) / 1000.0

        return OptionComponents(
            underlying_symbol=underlying,
            expiration_date=expiration,
            option_type=option_type,
            strike_price=strike,
        )

    except (ValueError, OverflowError, IndexError):
        return None


def convert_nanosecond_timestamp(ns_timestamp: int) -> datetime:
    """
    Convert a nanosecond timestamp to a Python datetime.

    Polygon API returns timestamps in nanoseconds since Unix epoch.
    Example: 1602648000000000000 → 2020-10-14 00:00:00

    Args:
        ns_timestamp: Timestamp in nanoseconds since Unix epoch

    Returns:
        Python datetime object in UTC

    Examples:
        >>> convert_nanosecond_timestamp(1602648000000000000)
        datetime.datetime(2020, 10, 14, 0, 0)

        >>> convert_nanosecond_timestamp(1697601600000000000)
        datetime.datetime(2023, 10, 18, 0, 0)
    """
    # Convert nanoseconds to seconds
    seconds = ns_timestamp / 1_000_000_000
    return datetime.utcfromtimestamp(seconds)


def convert_millisecond_timestamp(ms_timestamp: int) -> datetime:
    """
    Convert a millisecond timestamp to a Python datetime.

    Some Polygon API endpoints return timestamps in milliseconds.

    Args:
        ms_timestamp: Timestamp in milliseconds since Unix epoch

    Returns:
        Python datetime object in UTC
    """
    seconds = ms_timestamp / 1000
    return datetime.utcfromtimestamp(seconds)


def format_strike_price(strike: float) -> str:
    """
    Format a strike price for display.

    Args:
        strike: Strike price as a float

    Returns:
        Formatted string with 2 decimal places

    Examples:
        >>> format_strike_price(145.0)
        '$145.00'

        >>> format_strike_price(150.50)
        '$150.50'
    """
    return f"${strike:.2f}"


def extract_underlying_from_option_ticker(ticker: str) -> Optional[str]:
    """
    Quickly extract just the underlying symbol from an options ticker.

    This is faster than full parsing when only the underlying is needed.
    Parses from right to left to handle variable-length symbols.
    Extracts only A-Z letters, discarding any digits.

    Args:
        ticker: The options ticker string

    Returns:
        Underlying symbol (A-Z letters only) or None if parsing fails

    Examples:
        >>> extract_underlying_from_option_ticker("O:AAPL210917C00145000")
        'AAPL'

        >>> extract_underlying_from_option_ticker("O:ACB1260116C00001000")
        'ACB'

        >>> extract_underlying_from_option_ticker("O:AQMS1251017P00002500")
        'AQMS'
    """
    # Validate minimum length and prefix
    if not ticker or len(ticker) < 18 or not ticker.startswith("O:"):
        return None

    # Remove "O:" prefix
    ticker_body = ticker[2:]

    # Validate body length
    if len(ticker_body) < 16:
        return None

    try:
        # Parse from right to left: last 15 chars are date(6) + type(1) + strike(8)
        # Everything before that is the potential underlying symbol
        underlying_raw = ticker_body[:-15]

        # Extract only A-Z letters for the underlying symbol
        underlying = re.sub(r'[^A-Z]', '', underlying_raw.upper())

        # Validate we have at least 1 letter
        if not underlying:
            return None

        return underlying

    except (IndexError, ValueError):
        return None
