import os

from loguru import logger


class DirectoryValidator:
    @staticmethod
    def create_directory_if_not_exists(directory_path: str) -> None:
        """
        Checks for the existence of a directory or creates one if it doesn't exist.

        :param directory_path: The path to the directory to check or create.
        """
        if os.path.exists(directory_path):
            return directory_path
        try:
            os.makedirs(directory_path)
            logger.info(f"Directory created: {directory_path}")
        except Exception as e:
            logger.error(f"Error creating directory {directory_path}: {e}")
            raise
