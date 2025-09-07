import os
import yaml
from typing import Dict, List, Optional
from datetime import datetime

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
        
        # Загружаем данные о тикерах и ребалансировках
        self._tickers_data = self._load_tickers_data()

    def _load_tickers_data(self) -> Dict[str, Dict]:
        """
        Загружает данные о тикерах и ребалансировках из YAML файла
        
        Returns:
            Dict[str, Dict]: Словарь с именами портфелей как ключами и данными портфелей как значениями
        """
        try:
            with open(self.PATH_TO_VALIDATE["tickers_in_portfolio"], 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file)
                
                # Фильтруем ключи, не относящиеся к портфелям (например, start_date)
                portfolios = {}
                for k, v in data.items():
                    if k.startswith('portfolio_'):
                        portfolios[k] = v
                
                logger.info("Успешно загружены данные о тикерах и ребалансировках из YAML файла")
                return portfolios
        except FileNotFoundError:
            logger.error(f"Файл не найден: {self.PATH_TO_VALIDATE['tickers_in_portfolio']}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Ошибка парсинга YAML файла: {e}")
            return {}
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке данных о тикерах: {e}")
            return {}

    def get_portfolios(self) -> Dict[str, Dict]:
        """
        Получает все портфели с их данными
        
        Returns:
            Dict[str, Dict]: Словарь с именами портфелей как ключами и данными портфелей как значениями
        """
        logger.info(f"Получено {len(self._tickers_data)} портфелей")
        return self._tickers_data.copy()
    
    def get_portfolio_rebalance_dates(self, portfolio_name: str) -> List[str]:
        """
        Получает все даты ребалансировки для портфеля
        
        Args:
            portfolio_name (str): Имя портфеля
            
        Returns:
            List[str]: Список дат ребалансировки в формате YYYY-MM-DD
        """
        if portfolio_name not in self._tickers_data:
            logger.error(f"Портфель '{portfolio_name}' не найден")
            return []
        
        portfolio_data = self._tickers_data[portfolio_name]
        rebalance_dates = []
        
        # Ищем все ключи, начинающиеся с 'rebalance_date_'
        for key, value in portfolio_data.items():
            if key.startswith('rebalance_date_') and isinstance(value, dict) and 'date' in value:
                rebalance_dates.append(value['date'])
        
        # Сортируем даты по возрастанию
        rebalance_dates.sort()
        
        logger.info(f"Найдено {len(rebalance_dates)} дат ребалансировки для портфеля '{portfolio_name}'")
        return rebalance_dates
    
    def get_portfolio_tickers_for_date(self, portfolio_name: str, target_date: str) -> List[str]:
        """
        Получает тикеры портфеля на определенную дату (с учетом ребалансировок)
        
        Args:
            portfolio_name (str): Имя портфеля
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            List[str]: Список тикеров для указанной даты
        """
        if portfolio_name not in self._tickers_data:
            logger.error(f"Портфель '{portfolio_name}' не найден")
            return []
        
        portfolio_data = self._tickers_data[portfolio_name]
        
        # Находим актуальную ребалансировку для указанной даты
        active_rebalance = self._get_active_rebalance_for_date(portfolio_data, target_date)
        
        if not active_rebalance:
            logger.warning(f"Не найдена ребалансировка для портфеля '{portfolio_name}' на дату {target_date}")
            return []
        
        tickers = list(active_rebalance.get('tickers', {}).keys())
        logger.info(f"Получено {len(tickers)} тикеров для портфеля '{portfolio_name}' на дату {target_date}")
        return tickers
    
    def get_portfolio_weights_for_date(self, portfolio_name: str, target_date: str) -> Dict[str, float]:
        """
        Получает веса портфеля на определенную дату (с учетом ребалансировок)
        
        Args:
            portfolio_name (str): Имя портфеля
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            Dict[str, float]: Словарь с тикерами как ключами и весами как значениями
        """
        if portfolio_name not in self._tickers_data:
            logger.error(f"Портфель '{portfolio_name}' не найден")
            return {}
        
        portfolio_data = self._tickers_data[portfolio_name]
        
        # Находим актуальную ребалансировку для указанной даты
        active_rebalance = self._get_active_rebalance_for_date(portfolio_data, target_date)
        
        if not active_rebalance:
            logger.warning(f"Не найдена ребалансировка для портфеля '{portfolio_name}' на дату {target_date}")
            return {}
        
        weights = active_rebalance.get('tickers', {})
        # Фильтруем None значения
        weights = {ticker: weight for ticker, weight in weights.items() if weight is not None}
        
        if weights:
            logger.info(f"Получено {len(weights)} весов для портфеля '{portfolio_name}' на дату {target_date}")
        else:
            logger.info(f"Веса не указаны для портфеля '{portfolio_name}' на дату {target_date} - будет использоваться расчет на основе капитализации")
        
        return weights
    
    def has_portfolio_weights_for_date(self, portfolio_name: str, target_date: str) -> bool:
        """
        Проверяет, есть ли указанные веса для портфеля на определенную дату
        
        Args:
            portfolio_name (str): Имя портфеля
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            bool: True если веса указаны, False иначе
        """
        weights = self.get_portfolio_weights_for_date(portfolio_name, target_date)
        return len(weights) > 0
    
    def _get_active_rebalance_for_date(self, portfolio_data: Dict, target_date: str) -> Optional[Dict]:
        """
        Находит активную ребалансировку для указанной даты
        
        Args:
            portfolio_data (Dict): Данные портфеля
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            Optional[Dict]: Данные активной ребалансировки или None
        """
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')
        active_rebalance = None
        active_date = None
        
        # Ищем все ребалансировки
        for key, value in portfolio_data.items():
            if key.startswith('rebalance_date_') and isinstance(value, dict) and 'date' in value:
                rebalance_date = value['date']
                rebalance_dt = datetime.strptime(rebalance_date, '%Y-%m-%d')
                
                # Если дата ребалансировки <= целевой даты и она самая поздняя из подходящих
                if rebalance_dt <= target_dt and (active_date is None or rebalance_dt > active_date):
                    active_rebalance = value
                    active_date = rebalance_dt
        
        return active_rebalance

    # Методы для обратной совместимости со старой структурой
    def get_portfolio_tickers(self, portfolio_name: str) -> List[str]:
        """
        Получает тикеры портфеля (для обратной совместимости)
        Использует первую доступную ребалансировку
        
        Args:
            portfolio_name (str): Имя портфеля
            
        Returns:
            List[str]: Список тикеров для портфеля
        """
        rebalance_dates = self.get_portfolio_rebalance_dates(portfolio_name)
        if not rebalance_dates:
            logger.error(f"Портфель '{portfolio_name}' не найден или не содержит ребалансировок")
            return []
        
        # Используем первую дату ребалансировки
        first_date = rebalance_dates[0]
        return self.get_portfolio_tickers_for_date(portfolio_name, first_date)
    
    def get_portfolio_weights(self, portfolio_name: str) -> Dict[str, float]:
        """
        Получает веса портфеля (для обратной совместимости)
        Использует первую доступную ребалансировку
        
        Args:
            portfolio_name (str): Имя портфеля
            
        Returns:
            Dict[str, float]: Словарь с тикерами как ключами и весами как значениями
        """
        rebalance_dates = self.get_portfolio_rebalance_dates(portfolio_name)
        if not rebalance_dates:
            logger.error(f"Портфель '{portfolio_name}' не найден или не содержит ребалансировок")
            return {}
        
        # Используем первую дату ребалансировки
        first_date = rebalance_dates[0]
        return self.get_portfolio_weights_for_date(portfolio_name, first_date)
    
    def has_portfolio_weights(self, portfolio_name: str) -> bool:
        """
        Проверяет, есть ли указанные веса для портфеля (для обратной совместимости)
        Использует первую доступную ребалансировку
        
        Args:
            portfolio_name (str): Имя портфеля
            
        Returns:
            bool: True если веса указаны, False иначе
        """
        rebalance_dates = self.get_portfolio_rebalance_dates(portfolio_name)
        if not rebalance_dates:
            return False
        
        # Используем первую дату ребалансировки
        first_date = rebalance_dates[0]
        return self.has_portfolio_weights_for_date(portfolio_name, first_date)
