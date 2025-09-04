import concurrent.futures
from datetime import datetime
from typing import Optional, Dict, List

import requests
import pandas as pd
import yfinance as yf
from loguru import logger
from tenacity import retry, stop_after_delay, wait_fixed


class NYSEDataLoader:
    """
    A class for loading financial ticker data from NYSE using yfinance and EODHD libraries.
    Uses concurrent programming with threads for efficient data loading.
    """

    @staticmethod
    def load_ticker_data_eodhd(
        ticker: str, start_date: str, end_date: str, api_token: str, interval: str = "d"
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data from the EODHD API.

        :param ticker: The ticker symbol of the stock.
        :param start_date: Start date for the data in 'YYYY-MM-DD' format.
        :param end_date: End date for the data in 'YYYY-MM-DD' format.
        :param api_token: API token for EODHD.
        :param interval: Time interval for data (e.g., 'd' for daily, 'w' for weekly).
        :return: A DataFrame with the ticker data or None if an error occurs.
        """
        try:
            url = f"https://eodhistoricaldata.com/api/eod/{ticker}"
            params = {
                "api_token": api_token,
                "from": start_date,
                "to": end_date,
                "period": interval,
                "fmt": "json",
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                logger.warning(
                    f"No data found for ticker {ticker} in the specified range."
                )
                return None

            df = pd.DataFrame(data)

            required_columns = ["date", "open", "high", "low", "close", "volume"]
            if not set(required_columns).issubset(df.columns):
                logger.warning(f"Unexpected data format for ticker {ticker}.")
                return None

            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df.set_index("date", inplace=True)

            df = df[["open", "high", "low", "close", "volume"]].apply(
                pd.to_numeric, errors="coerce"
            )

            return df

        except Exception as e:
            logger.error(f"Error loading data for ticker {ticker} via EODHD: {e}")
            return None

    @staticmethod
    def load_ticker_data_yfinance(
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d",
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data from the yfinance library.

        :param ticker: The ticker symbol of the stock.
        :param start_date: Start date for the data in 'YYYY-MM-DD' format.
        :param end_date: End date for the data in 'YYYY-MM-DD' format.
        :param interval: Time interval for data (e.g., '1d' for daily, '1wk' for weekly).
        :return: A DataFrame with the ticker data or None if an error occurs.
        """
        try:
            if start_date is None:
                start_date = (pd.Timestamp.today() - pd.Timedelta(days=365)).strftime(
                    "%Y-%m-%d"
                )
            if end_date is None:
                end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

            df = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                interval=interval,
                progress=False,
            )

            if df.empty:
                logger.warning(
                    f"No data found for ticker {ticker} within the specified range."
                )
                return None

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df.reset_index(inplace=True)
            df.rename(
                columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                },
                inplace=True,
            )
            df = df[["date", "open", "high", "low", "close", "volume"]]
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df.set_index("date", inplace=True)
            df = df.apply(pd.to_numeric, errors="coerce")

            return df

        except Exception as e:
            logger.error(f"Error loading data for ticker {ticker} via yfinance: {e}")
            return None

    @staticmethod
    def _convert_interval(interval: str, for_yfinance: bool = False) -> str:
        """
        Converts between interval formats used by EODHD and yfinance.

        :param interval: Interval string ('d', 'w', 'm', etc.)
        :param for_yfinance: If True, converts from EODHD format to yfinance format
        :return: Converted interval string
        """
        if for_yfinance:
            conversion = {"d": "1d", "w": "1wk", "m": "1mo"}
            return conversion.get(interval, "1d")
        else:
            conversion = {"1d": "d", "1wk": "w", "1mo": "m"}
            return conversion.get(interval, "d")

    @staticmethod
    @retry(stop=stop_after_delay(30), wait=wait_fixed(2))
    def load_ticker_data(
        ticker: str,
        api_token_eodhd: str,
        start_date: pd.Timestamp = pd.Timestamp.today() - pd.Timedelta(days=365),
        end_date: pd.Timestamp = pd.Timestamp(datetime.now().strftime("%Y-%m-%d")),
        interval: str = "d",
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data using yfinance as the primary source.

        :param ticker: The ticker symbol of the stock.
        :param api_token_eodhd: API token for EODHD (not used anymore).
        :param start_date: Start date for the data as a pandas Timestamp.
        :param end_date: End date for the data as a pandas Timestamp.
        :param interval: Time interval for data (e.g., 'd' for daily, 'w' for weekly).
        :return: A DataFrame with the ticker data or None if both sources fail.
        :raises: RuntimeError if no data can be retrieved from any source
        """
        formatted_start = start_date.strftime("%Y-%m-%d")
        formatted_end = end_date.strftime("%Y-%m-%d")

        # Try yfinance first
        try:
            yf_interval = NYSEDataLoader._convert_interval(interval, for_yfinance=True)
            df = NYSEDataLoader.load_ticker_data_yfinance(
                ticker, formatted_start, formatted_end, interval=yf_interval
            )
            if df is not None and not df.empty:
                logger.info(
                    f"Successfully loaded data for ticker {ticker} from yfinance"
                )
                return df
            logger.warning(f"Failed to load data for ticker {ticker} from yfinance")
        except Exception as e:
            logger.warning(f"Error loading data for ticker {ticker} from yfinance: {e}")

        # If yfinance fails, try EODHD as fallback
        try:
            df = NYSEDataLoader.load_ticker_data_eodhd(
                ticker,
                formatted_start,
                formatted_end,
                api_token=api_token_eodhd,
                interval=interval,
            )
            if df is not None and not df.empty:
                logger.info(f"Successfully loaded data for ticker {ticker} from EODHD")
                return df
            logger.warning(f"Failed to load data for ticker {ticker} from EODHD")
        except Exception as e:
            logger.warning(f"Error loading data for ticker {ticker} from EODHD: {e}")

        # If both sources failed, raise an error that will trigger retry
        error_msg = f"Failed to load data for ticker {ticker} from both sources (yfinance and EODHD)"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    @staticmethod
    def load_all_tickers(
        tickers: List[str],
        api_token_eodhd: str,
        start_date: pd.Timestamp = pd.Timestamp.today() - pd.Timedelta(days=365 * 2),
        end_date: pd.Timestamp = pd.Timestamp(datetime.now().strftime("%Y-%m-%d")),
        interval: str = "d",
        max_workers: int = 1,
    ) -> Dict[str, pd.DataFrame]:
        """
        Downloads data for a list of stock tickers using thread pooling for parallel execution.
        Each ticker is associated with its respective DataFrame in the returned dictionary.

        :param tickers: A list of ticker symbols for which to download data (e.g., ['AAPL', 'MSFT']).
        :param api_token_eodhd: API token for EODHD.
        :param start_date: Start date as a pandas Timestamp.
        :param end_date: End date as a pandas Timestamp.
        :param interval: Data interval (e.g., 'd', 'w', 'm'). Defaults to 'd'.
        :param max_workers: Maximum number of worker threads to use for concurrent downloads.
        :return: A dictionary containing tickers as keys and their corresponding DataFrames as values.
        :raises: RuntimeError if no data can be retrieved for any ticker
        """
        try:
            quotations_dict = {}

            def process_ticker(ticker: str):
                try:
                    df = NYSEDataLoader.load_ticker_data(
                        ticker=ticker,
                        api_token_eodhd=api_token_eodhd,
                        start_date=start_date,
                        end_date=end_date,
                        interval=interval,
                    )
                    return (ticker, df)
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {e}")
                    raise

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_ticker = {
                    executor.submit(process_ticker, ticker): ticker
                    for ticker in tickers
                }

                for future in concurrent.futures.as_completed(future_to_ticker):
                    try:
                        ticker, df = future.result()
                        if df is not None and not df.empty:
                            quotations_dict[ticker] = df
                            logger.info(
                                f"Successfully downloaded data for stock ticker {ticker}"
                            )
                        else:
                            error_msg = f"No data for stock ticker {ticker}"
                            logger.error(error_msg)
                            raise RuntimeError(error_msg)
                    except Exception as e:
                        ticker = future_to_ticker[future]
                        logger.error(f"Failed to get result for ticker {ticker}: {e}")
                        raise

            if not quotations_dict:
                error_msg = (
                    "No data was retrieved for any of the provided stock tickers"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            return quotations_dict

        except Exception as e:
            logger.error(f"Failed to download data for stock tickers: {str(e)}")
            raise
