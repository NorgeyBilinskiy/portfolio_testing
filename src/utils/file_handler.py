import json
import yaml
from typing import Any

import pandas as pd
from loguru import logger


class FileHandler:
    @staticmethod
    def save_json(
        data: Any, file_path: str, text_successful: str, text_error: str
    ) -> None:
        """
        Saves data in JSON format to a file.

        :param data: The data to save (can be any type serializable to JSON).
        :param file_path: Path to the file where the data should be saved.
        :param text_successful: The success message to log.
        :param text_error: The error message to log in case of failure.
        """
        try:
            with open(file_path, "w") as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
            logger.info(text_successful)
        except Exception as e:
            logger.error(f"{text_error} {str(e)}")
            raise

    @staticmethod
    def read_json(file_path: str) -> Any:
        """
        Reads data from a JSON file.

        :param file_path: The path to the JSON file.
        :return: The data read from the file, can be any type that was stored in the JSON file.
        """
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
            return data
        except Exception as e:
            logger.error(f"Error reading data from {file_path}: {e}")
            raise

    @staticmethod
    def save_csv(
        data: Any, file_path: str, text_successful: str, text_error: str
    ) -> None:
        """
        Saves data in CSV format to a file.

        :param data: The data to save (must be an object that supports conversion to a DataFrame).
        :param file_path: Path to the file where the data should be saved.
        :param text_successful: The success message to log.
        :param text_error: The error message to log in case of failure.
        """
        try:
            if isinstance(data, pd.DataFrame):
                data.to_csv(file_path, index=True)
            else:
                pd.DataFrame(data).to_csv(file_path, index=False)
            logger.info(text_successful)
        except Exception as e:
            logger.error(f"{text_error} {str(e)}")
            raise

    @staticmethod
    def read_csv(file_path: str) -> pd.DataFrame:
        """
        Reads data from a CSV file.

        :param file_path: The path to the CSV file.
        :return: A pandas DataFrame containing the data from the CSV file.
        :raises Exception: If there are any issues reading the file.
        """
        try:
            data = pd.read_csv(file_path)
            return data
        except Exception as e:
            logger.error(f"Error reading data from {file_path}: {e}")
            raise

    @staticmethod
    def load_yaml(file_path: str) -> dict:
        """
        Loads data from a YAML file.

        Parameters:
        file_path (str): Path to the YAML file.

        Returns:
        dict: A dictionary with the loaded data.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = yaml.safe_load(file)
            return data
        except Exception as e:
            logger.error(f"Error reading YAML file at {file_path}: {e}")
            raise
