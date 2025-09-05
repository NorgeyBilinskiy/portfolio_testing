from typing import Dict, Any, List
import pandas as pd
from loguru import logger

from .config import Config
from .loading_quotations.moex import MoexDataLoader


class QuotationsProcessor:
    """
    A class to load and process stock quotations from Moscow Exchange for portfolio tickers.
    """

    def __init__(self):
        """
        Initializes the processor with configuration data.
        """
        self.config = Config()
        self.portfolios = self.config.get_portfolios()
        self.start_date = self.config.get_start_date()
        self.moex_loader = MoexDataLoader()

    def _process_dataframe(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Processes a given dataframe filtering by start date.

        Args:
            df (pd.DataFrame): The dataframe containing financial data to be processed.
            ticker (str): Ticker symbol.

        Returns:
            pd.DataFrame: The processed dataframe with today's data, weekend data removed, and filtered by start date.
        """
        if df.empty:
            return df

        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index, format="%Y-%m-%d")
            except:
                logger.warning(f"Could not convert index to datetime for ticker {ticker}")
                return df

        start_date = pd.to_datetime(self.start_date).date()
        df = df[df.index.date >= start_date]
        
        if df.empty:
            logger.warning(f"No data available for ticker {ticker} after start date {self.start_date}")
            return df

        df.index = df.index.strftime("%Y-%m-%d")
        
        logger.info(f"Processed {len(df)} trading days for ticker {ticker} (from {self.start_date})")
        return df

    def get_portfolio_quotations(self, portfolio_name: str) -> Dict[str, pd.DataFrame]:
        """
        Get quotations for a specific portfolio.

        Args:
            portfolio_name (str): Name of the portfolio (e.g., 'portfolio_1', 'portfolio_2').

        Returns:
            Dict[str, pd.DataFrame]: Dictionary with tickers as keys and DataFrames as values.
        """
        if portfolio_name not in self.portfolios:
            logger.error(f"Portfolio '{portfolio_name}' not found")
            return {}

        tickers = self.portfolios[portfolio_name]
        logger.info(f"Loading quotations for portfolio '{portfolio_name}' with {len(tickers)} tickers")

        quotations_dict = {}

        for ticker in tickers:
            try:
                # Load data using the correct method with start date from config
                start_date = pd.to_datetime(self.start_date)
                end_date = pd.Timestamp.today()
                
                ticker_data = self.moex_loader.load_ticker_data(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                    period=24  # Daily data
                )

                if ticker_data is not None and not ticker_data.empty:
                    processed_df = self._process_dataframe(ticker_data, ticker)
                    quotations_dict[ticker] = processed_df
                    logger.info(f"Successfully loaded data for {ticker}: {len(processed_df)} records")
                else:
                    logger.warning(f"No data available for ticker {ticker}")

            except Exception as e:
                logger.error(f"Error loading data for ticker {ticker}: {e}")

        logger.info(f"Successfully loaded quotations for {len(quotations_dict)} out of {len(tickers)} tickers in portfolio '{portfolio_name}'")
        return quotations_dict

    def get_all_portfolios_quotations(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get quotations for all portfolios.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: Nested dictionary with portfolio names as outer keys,
            tickers as inner keys, and DataFrames as values.
        """
        logger.info("Loading quotations for all portfolios")
        
        all_quotations = {}
        
        for portfolio_name in self.portfolios.keys():
            portfolio_quotations = self.get_portfolio_quotations(portfolio_name)
            all_quotations[portfolio_name] = portfolio_quotations
            
        logger.info(f"Successfully loaded quotations for {len(all_quotations)} portfolios")
        return all_quotations
