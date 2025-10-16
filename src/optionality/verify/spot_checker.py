"""Spot-check verification system for split-adjusted prices using Delta Lake."""

from datetime import date, timedelta
from typing import Dict, Any
import random

import polars as pl

from optionality.polygon_client import PolygonClient
from optionality.splits.adjuster import get_tickers_with_splits, get_splits_for_ticker
from optionality.storage.delta_manager import delta
from optionality.validators import calculate_price_difference_percent
from optionality.logger import logger

# Tolerance for price comparison (0.1%)
PRICE_TOLERANCE_PERCENT = 0.1


class VerificationFailure(Exception):
    """Raised when verification fails."""

    pass


def spot_check_ticker(
    polygon_client: PolygonClient,
    ticker: str,
    check_dates: list[date],
) -> Dict[str, Any]:
    """
    Spot-check a single ticker's adjusted prices against Polygon API.

    Args:
        polygon_client: PolygonClient instance
        ticker: Stock ticker symbol
        check_dates: List of dates to verify

    Returns:
        Dictionary with verification results:
            - ticker: str
            - dates_checked: int
            - passed: int
            - failed: int
            - errors: int
            - details: List of check results
    """
    results = {
        "ticker": ticker,
        "dates_checked": 0,
        "passed": 0,
        "failed": 0,
        "errors": 0,
        "details": [],
    }

    for check_date in check_dates:
        # Get our adjusted price from Delta table
        adjusted_lf = delta.scan_stocks_adjusted(
            ticker=ticker,
            start_date=check_date,
            end_date=check_date,
        )

        our_prices_df = adjusted_lf.collect()

        if len(our_prices_df) == 0:
            results["errors"] += 1
            results["details"].append(
                {
                    "date": check_date,
                    "status": "error",
                    "message": "No data in our Delta tables",
                }
            )
            continue

        our_price = our_prices_df.row(0, named=True)

        # Get Polygon's adjusted price (adjusted=True)
        polygon_data = polygon_client.get_daily_aggregate(
            ticker, check_date, adjusted=True
        )

        if not polygon_data:
            results["errors"] += 1
            results["details"].append(
                {
                    "date": check_date,
                    "status": "error",
                    "message": "No data from Polygon API",
                }
            )
            continue

        # Compare adjusted close prices
        our_close = float(our_price["close"])
        polygon_close = float(polygon_data["close"])

        diff_percent = abs(calculate_price_difference_percent(our_close, polygon_close))

        results["dates_checked"] += 1

        if diff_percent <= PRICE_TOLERANCE_PERCENT:
            results["passed"] += 1
            results["details"].append(
                {
                    "date": check_date,
                    "status": "pass",
                    "our_price": our_close,
                    "polygon_price": polygon_close,
                    "diff_percent": diff_percent,
                }
            )
        else:
            results["failed"] += 1
            results["details"].append(
                {
                    "date": check_date,
                    "status": "fail",
                    "our_price": our_close,
                    "polygon_price": polygon_close,
                    "diff_percent": diff_percent,
                }
            )

    return results


def _select_check_dates_for_splits(splits_df: pl.DataFrame) -> list[date]:
    """
    Select dates to check for splits verification.

    For each split, checks:
    - Historical date (30 days before split)
    - Day before split execution
    - Split execution day
    - Day after split execution

    Args:
        splits_df: DataFrame containing split information

    Returns:
        Sorted list of unique dates to check
    """
    check_dates = []

    for split in splits_df.iter_rows(named=True):
        execution_date = split["execution_date"]

        # Add historical date (30+ days before split)
        historical_date = execution_date - timedelta(days=30)
        check_dates.append(historical_date)

        # Add day before split
        day_before = execution_date - timedelta(days=1)
        check_dates.append(day_before)

        # Add split execution day (critical!)
        check_dates.append(execution_date)

        # Add day after split
        day_after = execution_date + timedelta(days=1)
        check_dates.append(day_after)

    # Remove duplicates and sort
    return sorted(list(set(check_dates)))


def _print_ticker_summary(ticker: str, ticker_results: Dict[str, Any]) -> None:
    """Print summary for a single ticker's results."""
    if ticker_results["failed"] > 0:
        logger.error(
            f"  ❌ {ticker}: {ticker_results['passed']}/{ticker_results['dates_checked']} passed "
            f"({ticker_results['failed']} failed, {ticker_results['errors']} errors)"
        )
    else:
        logger.success(
            f"  ✅ {ticker}: {ticker_results['passed']}/{ticker_results['dates_checked']} passed "
            f"({ticker_results['errors']} errors)"
        )


def _print_overall_summary(overall_results: Dict[str, Any]) -> None:
    """Print overall summary of spot check results."""
    logger.info("=" * 60)
    logger.info("📊 Spot Check Summary")
    logger.info("=" * 60)
    logger.info(f"Tickers checked: {overall_results['tickers_checked']}")
    logger.info(f"Total date checks: {overall_results['total_checks']}")
    logger.success(f"✅ Passed: {overall_results['passed']}")

    if overall_results['failed'] > 0:
        logger.error(f"❌ Failed: {overall_results['failed']}")
    else:
        logger.info("✅ Failed: 0")

    if overall_results['errors'] > 0:
        logger.warning(f"⚠️ Errors: {overall_results['errors']}")
    else:
        logger.info("✅ Errors: 0")


def _print_failed_checks(overall_results: Dict[str, Any]) -> None:
    """Print detailed information about failed checks."""
    logger.error("❌ Failed Checks:")

    for ticker_result in overall_results["ticker_results"]:
        for detail in ticker_result["details"]:
            if detail["status"] == "fail":
                logger.error(
                    f"  {ticker_result['ticker']} | {detail['date']} | "
                    f"Our: ${detail['our_price']:.2f} | "
                    f"Polygon: ${detail['polygon_price']:.2f} | "
                    f"Diff: {detail['diff_percent']:.2f}%"
                )


def run_spot_checks(
    polygon_client: PolygonClient,
    num_tickers: int = 5,
    days_back: int = 365,
) -> Dict[str, Any]:
    """
    Run spot checks on random tickers with recent splits.

    Process:
    1. Get tickers with splits in the past year
    2. Select random sample
    3. For each ticker:
       - Check a historical date (before split)
       - Check day before split execution
       - Check split execution day
       - Check day after split execution
    4. Compare our adjusted prices vs Polygon's

    Args:
        polygon_client: PolygonClient instance
        num_tickers: Number of tickers to check
        days_back: How many days back to look for splits

    Returns:
        Dictionary with overall results and per-ticker details

    Raises:
        VerificationFailure: If any checks fail
    """
    logger.info(f"🔍 Running spot checks ({num_tickers} tickers)...")

    # Get tickers with splits in the past year
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    tickers_with_splits_df = get_tickers_with_splits(start_date, end_date)

    if len(tickers_with_splits_df) == 0:
        logger.warning("⚠️ No tickers with splits found")
        return {
            "tickers_checked": 0,
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "ticker_results": [],
        }

    # Select random sample
    sample_size = min(num_tickers, len(tickers_with_splits_df))
    selected_tickers = tickers_with_splits_df.sample(n=sample_size).to_dicts()

    logger.info(
        f"📊 Selected {sample_size} tickers from {len(tickers_with_splits_df)} with splits"
    )

    overall_results = {
        "tickers_checked": 0,
        "total_checks": 0,
        "passed": 0,
        "failed": 0,
        "errors": 0,
        "ticker_results": [],
    }

    # Check each ticker
    for ticker_info in selected_tickers:
        ticker = ticker_info["ticker"]
        logger.info(f"🔍 Checking {ticker}...")

        # Get all splits for this ticker
        splits_df = get_splits_for_ticker(ticker, start_date, end_date)

        if len(splits_df) == 0:
            logger.warning(f"  ⚠️ No splits found for {ticker}")
            continue

        # Select dates to check
        check_dates = _select_check_dates_for_splits(splits_df)
        logger.info(f"  📅 Checking {len(check_dates)} dates...")

        # Run spot checks
        ticker_results = spot_check_ticker(polygon_client, ticker, check_dates)

        # Aggregate results
        overall_results["tickers_checked"] += 1
        overall_results["total_checks"] += ticker_results["dates_checked"]
        overall_results["passed"] += ticker_results["passed"]
        overall_results["failed"] += ticker_results["failed"]
        overall_results["errors"] += ticker_results["errors"]
        overall_results["ticker_results"].append(ticker_results)

        # Print summary for this ticker
        _print_ticker_summary(ticker, ticker_results)

    # Print overall summary
    _print_overall_summary(overall_results)

    # Show detailed failure table if any failed
    if overall_results["failed"] > 0:
        _print_failed_checks(overall_results)

        raise VerificationFailure(
            f"{overall_results['failed']} spot checks failed! "
            "Adjusted prices do not match Polygon API within tolerance."
        )

    logger.success("✅ All spot checks passed!")

    return overall_results
