import os

from loguru import logger


class FileValidator:
    @staticmethod
    def validate_file_path(file_path: str) -> str:
        """
        Checks if the file path is valid and the file exists.

        :param file_path: The path to the file to validate.
        :return: The file path if the file exists.
        :raises FileNotFoundError: If the file does not exist.
        """
        if not os.path.isfile(file_path):
            logger.error(f"The file {file_path} does not exist.")
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        return file_path
