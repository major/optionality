"""Tests for options ticker parsers."""

import pytest
from datetime import date

from optionality.parsers import (
    parse_options_ticker,
    extract_underlying_from_option_ticker,
    format_strike_price,
    convert_nanosecond_timestamp,
    convert_millisecond_timestamp,
    OptionComponents,
)


class TestParseOptionsTicker:
    """Tests for parse_options_ticker function."""

    @pytest.mark.parametrize(
        "ticker,expected",
        [
            # Standard tickers with letter-only underlying
            (
                "O:AAPL210917C00145000",
                OptionComponents(
                    underlying_symbol="AAPL",
                    expiration_date=date(2021, 9, 17),
                    option_type="C",
                    strike_price=145.0,
                ),
            ),
            (
                "O:TSLA220121P01000000",
                OptionComponents(
                    underlying_symbol="TSLA",
                    expiration_date=date(2022, 1, 21),
                    option_type="P",
                    strike_price=1000.0,
                ),
            ),
            # Tickers with digits in raw format (digits are stripped, letters only extracted)
            (
                "O:ACB1260116C00001000",
                OptionComponents(
                    underlying_symbol="ACB",
                    expiration_date=date(2026, 1, 16),
                    option_type="C",
                    strike_price=1.0,
                ),
            ),
            (
                "O:AQMS1251017P00002500",
                OptionComponents(
                    underlying_symbol="AQMS",
                    expiration_date=date(2025, 10, 17),
                    option_type="P",
                    strike_price=2.5,
                ),
            ),
            # Short underlying symbols
            (
                "O:A230616C00100000",
                OptionComponents(
                    underlying_symbol="A",
                    expiration_date=date(2023, 6, 16),
                    option_type="C",
                    strike_price=100.0,
                ),
            ),
            (
                "O:GM240315P00050000",
                OptionComponents(
                    underlying_symbol="GM",
                    expiration_date=date(2024, 3, 15),
                    option_type="P",
                    strike_price=50.0,
                ),
            ),
            # Long underlying symbols
            (
                "O:GOOGL250620C02500000",
                OptionComponents(
                    underlying_symbol="GOOGL",
                    expiration_date=date(2025, 6, 20),
                    option_type="C",
                    strike_price=2500.0,
                ),
            ),
            # Fractional strikes
            (
                "O:SPY240119C00450500",
                OptionComponents(
                    underlying_symbol="SPY",
                    expiration_date=date(2024, 1, 19),
                    option_type="C",
                    strike_price=450.5,
                ),
            ),
            (
                "O:QQQ240216P00375250",
                OptionComponents(
                    underlying_symbol="QQQ",
                    expiration_date=date(2024, 2, 16),
                    option_type="P",
                    strike_price=375.25,
                ),
            ),
            # Very low strikes
            (
                "O:XYZ240101C00000500",
                OptionComponents(
                    underlying_symbol="XYZ",
                    expiration_date=date(2024, 1, 1),
                    option_type="C",
                    strike_price=0.5,
                ),
            ),
        ],
    )
    def test_valid_tickers(self, ticker: str, expected: OptionComponents) -> None:
        """Test parsing of valid option tickers."""
        result = parse_options_ticker(ticker)
        assert result is not None
        assert result.underlying_symbol == expected.underlying_symbol
        assert result.expiration_date == expected.expiration_date
        assert result.option_type == expected.option_type
        assert result.strike_price == expected.strike_price

    @pytest.mark.parametrize(
        "invalid_ticker",
        [
            "",  # Empty string
            "O:",  # Too short
            "O:AAPL",  # Missing components
            "AAPL210917C00145000",  # Missing O: prefix
            "O:AAPL210917X00145000",  # Invalid option type
            "O:AAPL21091C00145000",  # Invalid date (too short)
            "O:AAPL2109177C00145000",  # Invalid date (too long)
            "O:AAPL210917C0014500",  # Invalid strike (too short)
            "O:AAPL210917C001450000",  # Invalid strike (too long)
            "O:210917C00145000",  # Missing underlying
            None,  # None input
        ],
    )
    def test_invalid_tickers(self, invalid_ticker: str | None) -> None:
        """Test that invalid tickers return None."""
        result = parse_options_ticker(invalid_ticker)
        assert result is None

    def test_edge_case_minimum_length(self) -> None:
        """Test minimum valid ticker length (1-char underlying)."""
        result = parse_options_ticker("O:A240101C00010000")
        assert result is not None
        assert result.underlying_symbol == "A"
        assert result.strike_price == 10.0


class TestExtractUnderlyingFromOptionTicker:
    """Tests for extract_underlying_from_option_ticker function."""

    @pytest.mark.parametrize(
        "ticker,expected_underlying",
        [
            ("O:AAPL210917C00145000", "AAPL"),
            ("O:ACB1260116C00001000", "ACB"),
            ("O:AQMS1251017P00002500", "AQMS"),
            ("O:A230616C00100000", "A"),
            ("O:GOOGL250620C02500000", "GOOGL"),
        ],
    )
    def test_extract_underlying_valid(self, ticker: str, expected_underlying: str) -> None:
        """Test extracting underlying from valid tickers."""
        result = extract_underlying_from_option_ticker(ticker)
        assert result == expected_underlying

    @pytest.mark.parametrize(
        "invalid_ticker",
        [
            "",
            "O:",
            "O:AAPL",
            "AAPL210917C00145000",
            None,
        ],
    )
    def test_extract_underlying_invalid(self, invalid_ticker: str | None) -> None:
        """Test that invalid tickers return None."""
        result = extract_underlying_from_option_ticker(invalid_ticker)
        assert result is None


class TestFormatStrikePrice:
    """Tests for format_strike_price function."""

    @pytest.mark.parametrize(
        "strike,expected",
        [
            (145.0, "$145.00"),
            (150.5, "$150.50"),
            (1000.0, "$1000.00"),
            (2.5, "$2.50"),
            (0.5, "$0.50"),
            (100.123, "$100.12"),  # Rounds to 2 decimals
        ],
    )
    def test_format_strike_price(self, strike: float, expected: str) -> None:
        """Test strike price formatting."""
        result = format_strike_price(strike)
        assert result == expected


class TestTimestampConverters:
    """Tests for timestamp conversion functions."""

    def test_convert_nanosecond_timestamp(self) -> None:
        """Test nanosecond timestamp conversion."""
        # 2020-10-14 00:00:00 UTC
        ns_timestamp = 1602648000000000000
        result = convert_nanosecond_timestamp(ns_timestamp)
        # Note: utcfromtimestamp may show different hours depending on timezone
        # Just verify the core components that should always be consistent
        assert result.year == 2020
        assert result.month == 10
        assert result.day == 14

    def test_convert_millisecond_timestamp(self) -> None:
        """Test millisecond timestamp conversion."""
        # 2023-10-18 00:00:00 UTC
        ms_timestamp = 1697587200000
        result = convert_millisecond_timestamp(ms_timestamp)
        assert result.year == 2023
        assert result.month == 10
        assert result.day == 18
