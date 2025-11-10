import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(level=logging.INFO, log_dir: str = "logs"):
    """Настройка логирования для всего проекта
    [PRM-0] [2025-10-17 18:12:35] [file:34:func] - level - message
    """

    formatter = logging.Formatter(
        '[PRM-ENRICH] [%(asctime)s] [%(name)s:%(lineno)s:%(funcName)s] - %(levelname)s - %(message)s'
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(level)

    logger.handlers.clear()
    logger.addHandler(console_handler)
    return logger


setup_logging()
