import concurrent.futures
import ssl
import urllib3
from datetime import datetime
from typing import Optional, Dict, List

import requests
import pandas as pd
import apimoex
from moexalgo import Ticker
from loguru import logger
from tenacity import retry, stop_after_delay, wait_fixed, retry_if_exception_type

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MoexDataLoader:
    """
    A class for loading financial ticker data from MOEX using moexalgo or apimoex libraries.
    Uses concurrent programming with threads for efficient data loading.
    """

    # Custom SSL context for better compatibility
    @staticmethod
    def _create_ssl_context():
        """Create a custom SSL context with relaxed settings for compatibility."""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    @staticmethod
    def _create_session():
        """Create a requests session with custom settings."""
        session = requests.Session()
        session.verify = False  # Disable SSL verification for compatibility
        session.timeout = (10, 30)  # (connect_timeout, read_timeout)

        # Custom SSL adapter
        adapter = requests.adapters.HTTPAdapter(
            max_retries=3, pool_connections=10, pool_maxsize=20
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @staticmethod
    @retry(
        stop=stop_after_delay(60 * 3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, ssl.SSLError)
        ),
    )
    def get_moex_indices() -> List[str]:
        """
        Fetches and returns a list of all Moscow Exchange (MOEX) indices.

        This function retrieves data from the MOEX ISS API to get a list of securities
        that are categorized as indices. It extracts the 'SECID' (security identifier)
        for each index and returns them as a list of strings.

        Returns:
            List[str]: A list of strings, where each string is the SECID of a MOEX index.

        Raises:
            requests.exceptions.RequestException: If there are issues connecting to the MOEX API
            KeyError: If the API response structure has changed
        """
        url: str = (
            "https://iss.moex.com/iss/engines/stock/markets/index/securities.json"
        )
        params: Dict[str, str] = {"iss.meta": "off", "iss.only": "securities"}

        try:
            session = MoexDataLoader._create_session()
            response: requests.Response = session.get(url, params=params)
            response.raise_for_status()
            data: dict = response.json()

            columns: List[str] = data["securities"]["columns"]
            try:
                secid_index: int = columns.index("SECID")
            except ValueError:
                secid_index: int = 0

            indices: List[str] = [
                item[secid_index] for item in data["securities"]["data"]
            ]
            logger.info("The MOEX index list has been successfully uploaded")
            return indices

        except (requests.exceptions.RequestException, ssl.SSLError) as e:
            logger.error(f"Error fetching data from MOEX API: {e}")
            raise

        except KeyError as e:
            logger.error(
                f"Error processing API response (KeyError): {e}. API structure might have changed."
            )
            raise

    @staticmethod
    def load_ticker_data_moexalgo(
        ticker: str, start_date: str, end_date: str, period: int = 24
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data from the moexalgo library.

        :param ticker: The ticker symbol of the stock.
        :param start_date: Start date for the data in 'YYYY-MM-DD' format.
        :param end_date: End date for the data in 'YYYY-MM-DD' format.
        :param period: Time interval for candles (24 for days, 7 for weeks).
        :return: A DataFrame with the ticker data or None if an error occurs.
        """
        try:
            ssl._create_default_https_context = ssl._create_unverified_context

            ticker_data = Ticker(ticker)
            df = ticker_data.candles(start=start_date, end=end_date, period=period)

            if df is None or df.empty:
                logger.warning(
                    f"No data found for ticker {ticker} in the specified range."
                )
                return None

            df.reset_index(inplace=True)
            df.rename(
                columns={
                    "begin": "date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume",
                },
                inplace=True,
            )
            df = df[["date", "open", "high", "low", "close", "volume"]]
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df.set_index("date", inplace=True)
            df = df.apply(pd.to_numeric, errors="coerce")

            return df

        except Exception as e:
            logger.error(f"Error loading data for ticker {ticker} via moexalgo: {e}")
            return None

    @staticmethod
    def load_ticker_data_apimoex(
        ticker: str, start_date: str, end_date: str, period: int = 24
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data from the apimoex library.

        :param ticker: The ticker symbol of the stock.
        :param start_date: Start date for the data in 'YYYY-MM-DD' format.
        :param end_date: End date for the data in 'YYYY-MM-DD' format.
        :param period: Time interval for candles (24 for days, 7 for weeks).
        :return: A DataFrame with the ticker data or None if an error occurs.
        """
        try:
            # Use custom session with better SSL handling
            session = MoexDataLoader._create_session()

            data = apimoex.get_market_candles(
                session,
                ticker,
                start=start_date,
                end=end_date,
                interval=period,
            )

            if not data:
                logger.warning(
                    f"No data found for ticker {ticker} in the specified range."
                )
                return None

            df = pd.DataFrame(data)

            required_columns = ["begin", "open", "high", "low", "close", "volume"]
            if not set(required_columns).issubset(df.columns):
                logger.warning(f"Unexpected data format for ticker {ticker}.")
                return None

            df["begin"] = pd.to_datetime(df["begin"]).dt.strftime("%Y-%m-%d")
            df.set_index("begin", inplace=True)

            df = df[["open", "high", "low", "close", "volume"]].apply(
                pd.to_numeric, errors="coerce"
            )

            return df

        except Exception as e:
            logger.error(f"Error loading data for ticker {ticker} via apimoex: {e}")
            return None

    @staticmethod
    @retry(
        stop=stop_after_delay(30),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(
            (RuntimeError, requests.exceptions.RequestException, ssl.SSLError)
        ),
    )
    def load_ticker_data(
        ticker: str,
        start_date: pd.Timestamp = pd.Timestamp.today() - pd.Timedelta(days=365),
        end_date: pd.Timestamp = pd.to_datetime(
            datetime.now().strftime("%d.%m.%Y"), dayfirst=True
        ),
        period: int = 24,
    ) -> Optional[pd.DataFrame]:
        """
        Load ticker data using moexalgo as the primary source and apimoex as the fallback.

        :param ticker: The ticker symbol of the stock.
        :param start_date: Start date for the data as a pandas Timestamp.
        :param end_date: End date for the data as a pandas Timestamp.
        :param period: Time interval for candles (24 for days, 7 for weeks).
        :return: A DataFrame with the ticker data or None if both sources fail.
        :raises: RuntimeError if no data can be retrieved from any source
        """
        # Try moexalgo first
        try:
            df = MoexDataLoader.load_ticker_data_moexalgo(
                ticker,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                period,
            )
            if df is not None and not df.empty:
                logger.info(
                    f"Successfully loaded data for ticker {ticker} from moexalgo"
                )
                return df
            logger.warning(
                f"Failed to load data for ticker {ticker} from moexalgo, trying apimoex"
            )
        except Exception as e:
            logger.warning(
                f"Error loading data for ticker {ticker} from moexalgo: {e}, trying apimoex"
            )

        # If moexalgo fails, try apimoex
        try:
            df = MoexDataLoader.load_ticker_data_apimoex(
                ticker,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                period,
            )
            if df is not None and not df.empty:
                logger.info(
                    f"Successfully loaded data for ticker {ticker} from apimoex"
                )
                return df
            logger.warning(f"Failed to load data for ticker {ticker} from apimoex")
        except Exception as e:
            logger.warning(f"Error loading data for ticker {ticker} from apimoex: {e}")

        # If both sources failed, raise an error that will trigger retry
        error_msg = f"Failed to load data for ticker {ticker} from both sources (moexalgo and apimoex)"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    @staticmethod
    def load_all_tickers(
        tickers: list[str],
        start_date: pd.Timestamp = pd.Timestamp.today() - pd.Timedelta(days=365 * 2),
        end_date: pd.Timestamp = pd.to_datetime(
            datetime.now().strftime("%d.%m.%Y"), dayfirst=True
        ),
        period: int = 24,
        max_workers: int = 30,  # Reduced from 100 to avoid overwhelming the API
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Downloads data for a list of tickers concurrently using thread pool executor.
        Each ticker is processed in a separate thread (up to max_workers threads).

        :param tickers: A list of ticker symbols for which to download data.
        :param start_date: Start date for the data as a pandas Timestamp.
        :param end_date: End date for the data as a pandas Timestamp.
        :param period: Time interval for candles (24 for days, 7 for weeks).
        :param max_workers: Maximum number of worker threads to use (default: 10).
        :return: A dictionary containing tickers as keys and their corresponding DataFrames as values.
        :raises: RuntimeError if no data can be retrieved for any ticker
        """
        try:
            quotations_dict = {}
            failed_tickers = []

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_ticker = {
                    executor.submit(
                        MoexDataLoader.load_ticker_data,
                        ticker=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        period=period,
                    ): ticker
                    for ticker in tickers
                }

                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            quotations_dict[ticker] = df
                            logger.info(
                                f"Successfully downloaded data for ticker {ticker}"
                            )
                        else:
                            failed_tickers.append(ticker)
                            logger.warning(f"No data for ticker {ticker}")
                    except Exception as e:
                        failed_tickers.append(ticker)
                        logger.error(f"Error processing ticker {ticker}: {str(e)}")

            # Log summary of results
            if quotations_dict:
                logger.info(
                    f"Successfully loaded data for {len(quotations_dict)} tickers: {list(quotations_dict.keys())}"
                )

            if failed_tickers:
                logger.warning(
                    f"Failed to load data for {len(failed_tickers)} tickers: {failed_tickers}"
                )

            # Return data even if some tickers failed, but log the failures
            if not quotations_dict:
                error_msg = "No data was retrieved for any of the provided tickers"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            return quotations_dict

        except Exception as e:
            logger.error(f"Failed to download data for quotations tickers: {str(e)}")
            raise
