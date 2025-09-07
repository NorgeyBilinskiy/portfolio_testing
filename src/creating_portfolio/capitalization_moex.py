from typing import Optional, Dict

import requests
import pandas as pd
from loguru import logger


class CapitalizationMOEX:
    """
    Class for getting actual market capitalization of companies from Moscow Exchange
    """
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/.json"
        self.market_df: Optional[pd.DataFrame] = None
    
    def get_all_capitalization_data(self) -> pd.DataFrame:
        """
        Gets market capitalization data for all companies from Moscow Exchange
        
        Returns:
            pd.DataFrame: DataFrame with SECID and ISSUECAPITALIZATION columns
        """
        try:
            response = requests.get(self.base_url)
            
            if response.status_code == 200:
                data = response.json()
                market_data = data['marketdata']['data']
                columns = data['marketdata']['columns']
                market_df = pd.DataFrame(market_data, columns=columns)
                market_df = market_df[['SECID', 'ISSUECAPITALIZATION']]

                self.market_df = market_df
                
                logger.info(f"Successfully retrieved data for {len(market_df)} companies")
                return market_df
            else:
                logger.error(f"Error retrieving data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error requesting API: {e}")
            return pd.DataFrame()
    
    def get_ticker_capitalization(self, ticker: str) -> Optional[float]:
        """
        Gets market capitalization for a specific ticker from DataFrame
        
        Args:
            ticker (str): Company ticker (e.g., 'SBER', 'GAZP')
            
        Returns:
            Optional[float]: Company capitalization in rubles or None if ticker not found
        """
        if self.market_df is None:
            logger.warning("Data not loaded. First call get_all_capitalization_data()")
            return None
        
        ticker_data = self.market_df[self.market_df['SECID'] == ticker]
        
        if ticker_data.empty:
            logger.warning(f"Ticker {ticker} not found in data")
            return None
        
        capitalization = ticker_data['ISSUECAPITALIZATION'].iloc[0]
        
        if pd.isna(capitalization) or capitalization == '':
            logger.warning(f"Capitalization for ticker {ticker} is not available")
            return None
        
        return float(capitalization)
    
    def get_multiple_tickers_capitalization(self, tickers: list) -> Dict[str, Optional[float]]:
        """
        Gets market capitalization for multiple tickers
        
        Args:
            tickers (list): List of tickers
            
        Returns:
            Dict[str, Optional[float]]: Dictionary with tickers and their capitalization
        """
        if self.market_df is None:
            logger.warning("Data not loaded. First call get_all_capitalization_data()")
            return {}
        
        result = {}
        for ticker in tickers:
            result[ticker] = self.get_ticker_capitalization(ticker)
        
        return result
