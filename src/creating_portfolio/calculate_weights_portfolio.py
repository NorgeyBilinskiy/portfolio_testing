from typing import Dict

from loguru import logger

from .capitalization_moex import CapitalizationMOEX
from ..config import Config


class IndexWeightsCalculator:
    """
    Class for calculating portfolio weights based on market capitalization
    """

    def __init__(self):
        """
        Initialize the calculator with configuration and capitalization data
        """
        self.config = Config()
        self.capitalization_loader = CapitalizationMOEX()
        self.portfolios = self.config.get_portfolios()
        
        logger.info("Loading market capitalization data...")
        self.capitalization_loader.get_all_capitalization_data()

    def calculate_portfolio_weights(self, portfolio_name: str) -> Dict[str, float]:
        """
        Calculate weights for a specific portfolio based on market capitalization
        
        Args:
            portfolio_name (str): Name of the portfolio
            
        Returns:
            Dict[str, float]: Dictionary with tickers as keys and weights as values
        """
        if portfolio_name not in self.portfolios:
            logger.error(f"Portfolio '{portfolio_name}' not found")
            return {}

        tickers = self.portfolios[portfolio_name]
        logger.info(f"Calculating weights for portfolio '{portfolio_name}' with {len(tickers)} tickers")

        capitalizations = self.capitalization_loader.get_multiple_tickers_capitalization(tickers)
        
        valid_capitalizations = {ticker: cap for ticker, cap in capitalizations.items() if cap is not None}
        
        if not valid_capitalizations:
            logger.warning(f"No valid capitalization data found for portfolio '{portfolio_name}'")
            return {}

        total_capitalization = sum(valid_capitalizations.values())
        
        if total_capitalization == 0:
            logger.warning(f"Total capitalization is zero for portfolio '{portfolio_name}'")
            return {}

        weights = {}
        for ticker, cap in valid_capitalizations.items():
            weight = cap / total_capitalization
            weights[ticker] = weight
            logger.debug(f"{ticker}: {cap:,.0f} rub. -> {weight:.4f} ({weight*100:.2f}%)")

        logger.info(f"Portfolio '{portfolio_name}' weights calculated: {len(weights)} tickers")
        logger.info(f"Total portfolio capitalization: {total_capitalization:,.0f} rub.")
        
        return weights

    def calculate_all_portfolios_weights(self) -> Dict[str, Dict[str, float]]:
        """
        Calculate weights for all portfolios
        
        Returns:
            Dict[str, Dict[str, float]]: Nested dictionary with portfolio names as outer keys,
            tickers as inner keys, and weights as values
        """
        logger.info("Calculating weights for all portfolios")
        
        all_weights = {}
        
        for portfolio_name in self.portfolios.keys():
            portfolio_weights = self.calculate_portfolio_weights(portfolio_name)
            all_weights[portfolio_name] = portfolio_weights
            
        logger.info(f"Successfully calculated weights for {len(all_weights)} portfolios")
        return all_weights
