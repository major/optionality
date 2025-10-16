"""Polygon API client wrapper with retry logic and rate limiting."""

from datetime import date, datetime
from typing import List, Dict, Any, Optional
import time

from polygon import RESTClient
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from optionality.config import get_settings
from optionality.logger import logger


class PolygonAPIError(Exception):
    """Raised when Polygon API returns an error."""

    pass


class PolygonClient:
    """Wrapper around Polygon REST client with retry logic."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Polygon client.

        Args:
            api_key: Polygon API key. If None, loads from config.
        """
        settings = get_settings()
        self.api_key = api_key or settings.polygon_api_key

        if not self.api_key:
            raise ValueError(
                "Polygon API key not configured. Set POLYGON_API_KEY in .env file."
            )

        self.client = RESTClient(api_key=self.api_key)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def get_all_splits(
        self,
        start_date: date,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL stock splits for ALL tickers within a date range.

        This uses Polygon's bulk splits endpoint which is much more efficient
        than querying per-ticker.

        Args:
            start_date: Start date for splits lookup (execution_date >= start_date)
            end_date: End date for splits lookup (None = today)

        Returns:
            List of split dictionaries with keys:
                - id: str (Polygon's unique split ID)
                - ticker: str
                - execution_date: date
                - split_from: float
                - split_to: float
                - split_factor: float (split_to / split_from)

        Example:
            >>> client = PolygonClient()
            >>> splits = client.get_all_splits(date(2020, 1, 1))
            >>> # Returns ALL splits for ALL tickers since 2020-01-01
        """
        if end_date is None:
            end_date = date.today()

        splits = []

        try:
            # Use Polygon's bulk splits endpoint - no ticker parameter needed!
            for split in self.client.list_splits(
                execution_date_gte=start_date.isoformat(),
                execution_date_lte=end_date.isoformat(),
                order="asc",
                limit=1000,  # Max results per request
            ):
                # Type ignore: Polygon SDK returns objects with these attributes
                ticker = split.ticker if hasattr(split, "ticker") else None  # type: ignore[attr-defined]
                split_id = getattr(split, "id", None)

                if not ticker or not split_id:
                    continue

                split_factor = split.split_to / split.split_from  # type: ignore[attr-defined, operator]

                splits.append(
                    {
                        "id": split_id,
                        "ticker": ticker,
                        "execution_date": datetime.strptime(
                            split.execution_date, "%Y-%m-%d"  # type: ignore[attr-defined, arg-type]
                        ).date(),
                        "split_from": split.split_from,  # type: ignore[attr-defined]
                        "split_to": split.split_to,  # type: ignore[attr-defined]
                        "split_factor": split_factor,
                    }
                )

        except Exception as e:
            logger.warning(f"  ⚠️ Error fetching bulk splits: {e}")

        return splits

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def get_splits(
        self,
        ticker: str,
        start_date: date,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch stock splits for a ticker within a date range.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            start_date: Start date for splits lookup
            end_date: End date for splits lookup (None = today)

        Returns:
            List of split dictionaries with keys:
                - id: str (Polygon's unique split ID)
                - ticker: str
                - execution_date: date
                - split_from: float
                - split_to: float
                - split_factor: float (split_to / split_from)

        Example:
            >>> client = PolygonClient()
            >>> splits = client.get_splits('AAPL', date(2020, 1, 1))
            >>> # Returns: [{'id': '...', 'ticker': 'AAPL', 'execution_date': date(2020, 8, 31),
            >>> #             'split_from': 1, 'split_to': 4, 'split_factor': 4.0}]
        """
        if end_date is None:
            end_date = date.today()

        splits = []

        try:
            # Use Polygon's stock splits endpoint
            for split in self.client.list_splits(
                ticker=ticker,
                execution_date_gte=start_date.isoformat(),
                execution_date_lte=end_date.isoformat(),
                order="asc",
                limit=1000,  # Max results per request
            ):
                # Type ignore: Polygon SDK returns objects with these attributes
                split_id = getattr(split, "id", None)

                if not split_id:
                    continue

                split_factor = split.split_to / split.split_from  # type: ignore[attr-defined, operator]

                splits.append(
                    {
                        "id": split_id,
                        "ticker": ticker,
                        "execution_date": datetime.strptime(
                            split.execution_date, "%Y-%m-%d"  # type: ignore[attr-defined, arg-type]
                        ).date(),
                        "split_from": split.split_from,  # type: ignore[attr-defined]
                        "split_to": split.split_to,  # type: ignore[attr-defined]
                        "split_factor": split_factor,
                    }
                )

        except Exception as e:
            # Log but don't fail - some tickers may not have splits
            logger.warning(f"  ⚠️ Error fetching splits for {ticker}: {e}")

        return splits

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def get_all_tickers(
        self,
        market: str = "stocks",
        active: bool = True,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all tickers from Polygon.

        Args:
            market: Market type ('stocks', 'options', 'crypto', etc.)
            active: Only fetch active tickers
            limit: Max results to fetch (default 1000)

        Returns:
            List of ticker dictionaries with metadata

        Example:
            >>> client = PolygonClient()
            >>> tickers = client.get_all_tickers(market='stocks', active=True)
        """
        tickers = []

        try:
            for ticker in self.client.list_tickers(
                market=market,
                active=active,
                limit=limit,
            ):
                # Type ignore: Polygon SDK returns objects with these attributes
                ticker_data = {
                    "ticker": ticker.ticker,  # type: ignore[attr-defined]
                    "name": ticker.name,  # type: ignore[attr-defined]
                    "market": ticker.market,  # type: ignore[attr-defined]
                    "locale": ticker.locale,  # type: ignore[attr-defined]
                    "primary_exchange": getattr(ticker, "primary_exchange", None),
                    "type": ticker.type,  # type: ignore[attr-defined]
                    "active": ticker.active,  # type: ignore[attr-defined]
                    "currency_name": getattr(ticker, "currency_name", None),
                    "currency_symbol": getattr(ticker, "currency_symbol", None),
                    "cik": getattr(ticker, "cik", None),
                    "composite_figi": getattr(ticker, "composite_figi", None),
                    "share_class_figi": getattr(ticker, "share_class_figi", None),
                    "last_updated_utc": getattr(ticker, "last_updated_utc", None),
                    "delisted_utc": getattr(ticker, "delisted_utc", None),
                }
                tickers.append(ticker_data)

        except Exception as e:
            logger.error(f"❌ Error fetching tickers: {e}")
            raise PolygonAPIError(f"Failed to fetch tickers: {e}")

        return tickers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def get_daily_aggregate(
        self,
        ticker: str,
        target_date: date,
        adjusted: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch daily aggregate (OHLCV) data for a specific date.

        Args:
            ticker: Stock ticker symbol
            target_date: The date to fetch data for
            adjusted: Whether to return split-adjusted prices (default True)

        Returns:
            Dictionary with OHLC data or None if not found:
                - ticker: str
                - date: date
                - open: float
                - high: float
                - low: float
                - close: float
                - volume: int
                - transactions: int

        Example:
            >>> client = PolygonClient()
            >>> data = client.get_daily_aggregate('AAPL', date(2023, 10, 20))
        """
        try:
            # Fetch aggregates for the specific date
            aggs = self.client.get_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="day",
                from_=target_date.isoformat(),
                to=target_date.isoformat(),
                adjusted=adjusted,
                limit=1,
            )

            # Type ignore: Polygon SDK may return HTTPResponse instead of list
            if not aggs or len(aggs) == 0:  # type: ignore[arg-type]
                return None

            agg = aggs[0]  # type: ignore[index]

            return {
                "ticker": ticker,
                "date": target_date,
                "open": agg.open,
                "high": agg.high,
                "low": agg.low,
                "close": agg.close,
                "volume": agg.volume,
                "transactions": getattr(agg, "transactions", None),
            }

        except Exception as e:
            logger.warning(f"  ⚠️ Error fetching aggregate for {ticker} on {target_date}: {e}")
            return None

    def get_splits_for_multiple_tickers(
        self,
        tickers: List[str],
        start_date: date,
        end_date: Optional[date] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch splits for multiple tickers (with rate limiting).

        Args:
            tickers: List of ticker symbols
            start_date: Start date for splits
            end_date: End date for splits

        Returns:
            Dictionary mapping ticker -> list of splits
        """
        results = {}

        for ticker in tickers:
            splits = self.get_splits(ticker, start_date, end_date)
            if splits:
                results[ticker] = splits

            # Rate limiting: sleep between requests
            time.sleep(0.1)  # 10 requests per second

        return results
