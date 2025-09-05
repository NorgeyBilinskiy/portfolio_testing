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

    def _load_tickers_data(self) -> Dict[str, List[str]]:
        """
        Load tickers data from YAML file
        
        Returns:
            Dict[str, List[str]]: Dictionary with portfolio names as keys and ticker lists as values
        """
        try:
            with open(self.PATH_TO_VALIDATE["tickers_in_portfolio"], 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                
                # Filter out non-portfolio keys (like start_date)
                portfolios = {k: v for k, v in data.items() if k.startswith('portfolio_')}
                
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

    def get_portfolios(self) -> Dict[str, List[str]]:
        """
        Get all portfolios with their tickers
        
        Returns:
            Dict[str, List[str]]: Dictionary with portfolio names as keys and ticker lists as values
        """
        logger.info(f"Retrieved {len(self._tickers_data)} portfolios")
        return self._tickers_data.copy()

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
