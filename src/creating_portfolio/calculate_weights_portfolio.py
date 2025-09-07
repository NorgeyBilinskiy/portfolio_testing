from typing import Dict, Optional

from loguru import logger

from .capitalization_moex import CapitalizationMOEX
from ..config import Config


class IndexWeightsCalculator:
    """
    Класс для расчета весов портфеля на основе рыночной капитализации и настроек ребалансировки
    """

    def __init__(self):
        """
        Инициализация калькулятора с конфигурацией и данными о капитализации
        """
        self.config = Config()
        self.capitalization_loader = CapitalizationMOEX()
        self.portfolios = self.config.get_portfolios()
        
        logger.info("Загрузка данных о рыночной капитализации...")
        self.capitalization_loader.get_all_capitalization_data()

    def calculate_portfolio_weights_for_date(self, portfolio_name: str, target_date: str) -> Dict[str, float]:
        """
        Рассчитывает веса портфеля на определенную дату с учетом ребалансировок
        
        Поддерживает смешанный расчет: часть весов из настроек, часть по капитализации.
        Если вес тикера = null, он рассчитывается по актуальной капитализации.
        
        Args:
            portfolio_name (str): Имя портфеля
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            Dict[str, float]: Словарь с тикерами как ключами и весами как значениями
        """
        if portfolio_name not in self.portfolios:
            logger.error(f"Портфель '{portfolio_name}' не найден")
            return {}

        # Получаем тикеры и веса для указанной даты
        tickers = self.config.get_portfolio_tickers_for_date(portfolio_name, target_date)
        predefined_weights = self.config.get_portfolio_weights_for_date(portfolio_name, target_date)
        
        if not tickers:
            logger.warning(f"Не найдены тикеры для портфеля '{portfolio_name}' на дату {target_date}")
            return {}

        logger.info(f"Расчет весов для портфеля '{portfolio_name}' на дату {target_date}")
        logger.info(f"Найдено {len(tickers)} тикеров, {len(predefined_weights)} с предустановленными весами")
        
        return self._calculate_mixed_weights(portfolio_name, target_date, tickers, predefined_weights)
    
    def calculate_portfolio_weights(self, portfolio_name: str) -> Dict[str, float]:
        """
        Рассчитывает веса портфеля (для обратной совместимости)
        Использует первую доступную ребалансировку
        
        Args:
            portfolio_name (str): Имя портфеля
            
        Returns:
            Dict[str, float]: Словарь с тикерами как ключами и весами как значениями
        """
        rebalance_dates = self.config.get_portfolio_rebalance_dates(portfolio_name)
        if not rebalance_dates:
            logger.error(f"Портфель '{portfolio_name}' не найден или не содержит ребалансировок")
            return {}
        
        # Используем первую дату ребалансировки
        first_date = rebalance_dates[0]
        return self.calculate_portfolio_weights_for_date(portfolio_name, first_date)
    
    def _calculate_mixed_weights(self, portfolio_name: str, target_date: str, tickers: list, predefined_weights: Dict[str, float]) -> Dict[str, float]:
        """
        Рассчитывает смешанные веса: часть из настроек, часть по капитализации
        
        Args:
            portfolio_name (str): Имя портфеля
            target_date (str): Целевая дата
            tickers (list): Список всех тикеров
            predefined_weights (Dict[str, float]): Предустановленные веса
            
        Returns:
            Dict[str, float]: Финальные веса портфеля
        """
        # Разделяем тикеры на две группы
        tickers_with_weights = list(predefined_weights.keys())
        tickers_without_weights = [ticker for ticker in tickers if ticker not in predefined_weights]
        
        logger.info(f"Тикеры с предустановленными весами: {tickers_with_weights}")
        logger.info(f"Тикеры для расчета по капитализации: {tickers_without_weights}")
        
        # Если все тикеры имеют предустановленные веса, просто нормализуем их
        if not tickers_without_weights:
            logger.info("Все тикеры имеют предустановленные веса, нормализуем их")
            return self._normalize_weights(predefined_weights)
        
        # Если ни один тикер не имеет предустановленных весов, рассчитываем все по капитализации
        if not tickers_with_weights:
            logger.info("Ни один тикер не имеет предустановленных весов, рассчитываем все по капитализации")
            return self._calculate_capitalization_weights_for_tickers(tickers)
        
        # Смешанный случай: часть весов из настроек, часть по капитализации
        logger.info("Смешанный расчет: часть весов из настроек, часть по капитализации")
        
        # Рассчитываем веса по капитализации для тикеров без предустановленных весов
        cap_weights = self._calculate_capitalization_weights_for_tickers(tickers_without_weights)
        
        if not cap_weights:
            logger.warning("Не удалось рассчитать веса по капитализации, используем только предустановленные")
            return self._normalize_weights(predefined_weights)
        
        # Сумма предустановленных весов
        predefined_sum = sum(predefined_weights.values())
        
        # Сумма весов по капитализации
        cap_sum = sum(cap_weights.values())
        
        # Нормализуем веса по капитализации пропорционально оставшемуся весу
        remaining_weight = 1.0 - predefined_sum
        
        if remaining_weight <= 0:
            logger.warning("Предустановленные веса >= 1.0, нормализуем только их")
            return self._normalize_weights(predefined_weights)
        
        # Масштабируем веса по капитализации
        scale_factor = remaining_weight / cap_sum
        scaled_cap_weights = {ticker: weight * scale_factor for ticker, weight in cap_weights.items()}
        
        # Объединяем веса
        final_weights = {**predefined_weights, **scaled_cap_weights}
        
        # Финальная нормализация
        final_weights = self._normalize_weights(final_weights)
        
        logger.info(f"Смешанный расчет завершен для портфеля '{portfolio_name}'")
        logger.info(f"Предустановленные веса: {predefined_sum:.4f}, по капитализации: {remaining_weight:.4f}")
        
        return final_weights
    
    def _calculate_capitalization_weights_for_tickers(self, tickers: list) -> Dict[str, float]:
        """
        Рассчитывает веса по капитализации для указанных тикеров
        
        Args:
            tickers (list): Список тикеров
            
        Returns:
            Dict[str, float]: Словарь с весами по капитализации
        """
        if not tickers:
            return {}
        
        logger.info(f"Расчет весов по капитализации для {len(tickers)} тикеров")

        capitalizations = self.capitalization_loader.get_multiple_tickers_capitalization(tickers)
        
        valid_capitalizations = {ticker: cap for ticker, cap in capitalizations.items() if cap is not None}
        
        if not valid_capitalizations:
            logger.warning("Не найдены валидные данные о капитализации")
            return {}

        total_capitalization = sum(valid_capitalizations.values())
        
        if total_capitalization == 0:
            logger.warning("Общая капитализация равна нулю")
            return {}

        weights = {}
        for ticker, cap in valid_capitalizations.items():
            weight = cap / total_capitalization
            weights[ticker] = weight
            logger.debug(f"{ticker}: {cap:,.0f} руб. -> {weight:.4f} ({weight*100:.2f}%)")

        logger.info(f"Рассчитаны веса по капитализации для {len(weights)} тикеров")
        logger.info(f"Общая капитализация: {total_capitalization:,.0f} руб.")
        
        return weights
    
    def _normalize_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        Нормализует веса так, чтобы их сумма равнялась 1.0
        
        Args:
            weights (Dict[str, float]): Словарь с весами
            
        Returns:
            Dict[str, float]: Нормализованные веса
        """
        if not weights:
            return {}
        
        total_weight = sum(weights.values())
        
        if total_weight == 0:
            logger.error("Общий вес равен нулю")
            return {}
        
        normalized_weights = {ticker: weight / total_weight for ticker, weight in weights.items()}
        
        logger.info(f"Веса нормализованы: {len(normalized_weights)} тикеров")
        logger.info(f"Общий вес до нормализации: {total_weight:.4f}")
        
        for ticker, weight in normalized_weights.items():
            logger.debug(f"{ticker}: {weight:.4f} ({weight*100:.2f}%)")
        
        return normalized_weights
    
    def calculate_all_portfolios_weights_for_date(self, target_date: str) -> Dict[str, Dict[str, float]]:
        """
        Рассчитывает веса для всех портфелей на определенную дату
        
        Args:
            target_date (str): Целевая дата в формате YYYY-MM-DD
            
        Returns:
            Dict[str, Dict[str, float]]: Вложенный словарь с именами портфелей как внешними ключами,
            тикерами как внутренними ключами и весами как значениями
        """
        logger.info(f"Расчет весов для всех портфелей на дату {target_date}")
        
        all_weights = {}
        
        for portfolio_name in self.portfolios.keys():
            portfolio_weights = self.calculate_portfolio_weights_for_date(portfolio_name, target_date)
            if portfolio_weights:  # Добавляем только если веса были успешно рассчитаны
                all_weights[portfolio_name] = portfolio_weights
            else:
                logger.warning(f"Пропускаем портфель '{portfolio_name}' из-за ошибок расчета")
        
        logger.info(f"Успешно рассчитаны веса для {len(all_weights)} портфелей")
        return all_weights

    def calculate_all_portfolios_weights(self) -> Dict[str, Dict[str, float]]:
        """
        Рассчитывает веса для всех портфелей (для обратной совместимости)
        Использует первую доступную ребалансировку для каждого портфеля
        
        Returns:
            Dict[str, Dict[str, float]]: Вложенный словарь с именами портфелей как внешними ключами,
            тикерами как внутренними ключами и весами как значениями
        """
        logger.info("Расчет весов для всех портфелей")
        
        all_weights = {}
        
        for portfolio_name in self.portfolios.keys():
            portfolio_weights = self.calculate_portfolio_weights(portfolio_name)
            if portfolio_weights:  # Добавляем только если веса были успешно рассчитаны
                all_weights[portfolio_name] = portfolio_weights
            else:
                logger.warning(f"Пропускаем портфель '{portfolio_name}' из-за ошибок расчета")
            
        logger.info(f"Успешно рассчитаны веса для {len(all_weights)} портфелей")
        return all_weights
