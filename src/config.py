import os
import yaml
from typing import Dict, List

from loguru import logger

from .utils import FileValidator


class Config:
    def __init__(self):
        # === Path Configurations ===
        self.BASE_DIR = os.getcwd()
        self.PATH_TO_VALIDATE = {
            "tickers_in_portfolio": os.path.join(
                self.BASE_DIR, "./settings", "tickers_in_portfolio.yaml"
            ),
        }
        
        # === Path Validation ===
        logger.info("Start checking for validity of paths to configuration files.")
        for name, path in self.PATH_TO_VALIDATE.items():
            FileValidator.validate_file_path(path)
        logger.info("All file paths have been validated successfully.")
        
        # Load tickers data
        self._tickers_data = self._load_tickers_data()

    def _load_tickers_data(self) -> Dict[str, Dict]:
        """
        Load tickers data from YAML file
        
        Returns:
            Dict[str, Dict]: Dictionary with portfolio names as keys and portfolio data as values
        """
        try:
            with open(self.PATH_TO_VALIDATE["tickers_in_portfolio"], 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                
                # Filter out non-portfolio keys (like start_date)
                portfolios = {}
                for k, v in data.items():
                    if k.startswith('portfolio_'):
                        portfolios[k] = v
                
                logger.info("Successfully loaded tickers data from YAML file")
                return portfolios
        except FileNotFoundError:
            logger.error(f"File not found: {self.PATH_TO_VALIDATE['tickers_in_portfolio']}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error loading tickers data: {e}")
            return {}

    def get_portfolios(self) -> Dict[str, Dict]:
        """
        Get all portfolios with their data
        
        Returns:
            Dict[str, Dict]: Dictionary with portfolio names as keys and portfolio data as values
        """
        logger.info(f"Retrieved {len(self._tickers_data)} portfolios")
        return self._tickers_data.copy()
    
    def get_portfolio_tickers(self, portfolio_name: str) -> List[str]:
        """
        Get tickers for a specific portfolio
        
        Args:
            portfolio_name (str): Name of the portfolio
            
        Returns:
            List[str]: List of tickers for the portfolio
        """
        if portfolio_name not in self._tickers_data:
            logger.error(f"Portfolio '{portfolio_name}' not found")
            return []
        
        portfolio_data = self._tickers_data[portfolio_name]
        
        # Check if it's new simplified format (dict with ticker: weight/null) or old format
        if isinstance(portfolio_data, dict):
            # New format: ticker -> weight/null
            if all(isinstance(v, (int, float, type(None))) for v in portfolio_data.values()):
                tickers = list(portfolio_data.keys())
            # Old format with tickers key
            elif 'tickers' in portfolio_data:
                tickers = portfolio_data['tickers']
            else:
                logger.error(f"Invalid portfolio format for '{portfolio_name}'")
                return []
        elif isinstance(portfolio_data, list):
            # Backward compatibility with old format
            tickers = portfolio_data
        else:
            logger.error(f"Invalid portfolio format for '{portfolio_name}'")
            return []
        
        logger.info(f"Retrieved {len(tickers)} tickers for portfolio '{portfolio_name}'")
        return tickers
    
    def get_portfolio_weights(self, portfolio_name: str) -> Dict[str, float]:
        """
        Get weights for a specific portfolio
        
        Args:
            portfolio_name (str): Name of the portfolio
            
        Returns:
            Dict[str, float]: Dictionary with tickers as keys and weights as values, 
                            empty dict if weights not specified
        """
        if portfolio_name not in self._tickers_data:
            logger.error(f"Portfolio '{portfolio_name}' not found")
            return {}
        
        portfolio_data = self._tickers_data[portfolio_name]
        
        # Check if it's new simplified format (dict with ticker: weight/null)
        if isinstance(portfolio_data, dict) and all(isinstance(v, (int, float, type(None))) for v in portfolio_data.values()):
            # Filter out null values (tickers without weights)
            weights = {ticker: weight for ticker, weight in portfolio_data.items() if weight is not None}
            if weights:
                logger.info(f"Retrieved {len(weights)} weights for portfolio '{portfolio_name}'")
                return weights
            else:
                logger.info(f"No weights specified for portfolio '{portfolio_name}' - will use capitalization-based calculation")
                return {}
        # Check if it's old format with weights key
        elif isinstance(portfolio_data, dict) and 'weights' in portfolio_data:
            weights = portfolio_data['weights']
            logger.info(f"Retrieved {len(weights)} weights for portfolio '{portfolio_name}'")
            return weights
        else:
            logger.info(f"No weights specified for portfolio '{portfolio_name}' - will use capitalization-based calculation")
            return {}
    
    def has_portfolio_weights(self, portfolio_name: str) -> bool:
        """
        Check if portfolio has specified weights
        
        Args:
            portfolio_name (str): Name of the portfolio
            
        Returns:
            bool: True if weights are specified, False otherwise
        """
        if portfolio_name not in self._tickers_data:
            return False
        
        portfolio_data = self._tickers_data[portfolio_name]
        
        # Check if it's new simplified format with any non-null weights
        if isinstance(portfolio_data, dict) and all(isinstance(v, (int, float, type(None))) for v in portfolio_data.values()):
            return any(weight is not None for weight in portfolio_data.values())
        # Check if it's old format with weights key
        elif isinstance(portfolio_data, dict) and 'weights' in portfolio_data:
            return True
        else:
            return False

    def get_start_date(self) -> str:
        """
        Get start date from YAML file
        
        Returns:
            str: Start date in YYYY-MM-DD format
        """
        try:
            with open(self.PATH_TO_VALIDATE["tickers_in_portfolio"], 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                start_date = data.get('start_date', '2015-01-01')
                logger.info(f"Retrieved start date: {start_date}")
                return start_date
        except FileNotFoundError:
            logger.error(f"File not found: {self.PATH_TO_VALIDATE['tickers_in_portfolio']}")
            return '2015-01-01'
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            return '2015-01-01'
        except Exception as e:
            logger.error(f"Unexpected error loading start date: {e}")
            return '2015-01-01'
