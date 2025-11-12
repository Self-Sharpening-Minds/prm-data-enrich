import logging
import sys
import os
from datetime import datetime


def setup_logging(log_dir: str = "logs"):
    """Настройка логирования.
    INFO и выше → в консоль,
    DEBUG/INFO/WARNING/ERROR → в файл logs/YYYY-MM-DD.log.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{datetime.now().date()}.log")

    formatter = logging.Formatter(
        '[PRM-ENRICH] [%(asctime)s] [%(filename)s:%(lineno)d:%(funcName)s] - %(levelname)s - %(message)s'
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.NOTSET)
    file_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"✅ Логирование инициализировано. Логи пишутся в: {log_file}")
    return logger


setup_logging()
