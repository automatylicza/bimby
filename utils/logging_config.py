# utils/logging_config.py

from loguru import logger
import logging
import sys
from typing import Optional

def configure_logging(
    log_file: str,
    log_level: str,
    rotation: str = "10 MB",
    compression: str = "zip",
    format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
):
    """
    Konfiguruje logowanie za pomocą Loguru i integruje standardowe logowanie z Loguru.

    :param log_file: Ścieżka do pliku logów.
    :param log_level: Poziom logowania (np. DEBUG, INFO).
    :param rotation: Maksymalny rozmiar pliku przed rotacją (np. "10 MB").
    :param compression: Typ kompresji dla starych logów (np. "zip").
    :param format: Format logów.
    """
    # Usunięcie domyślnych handlerów Loguru
    logger.remove()

    # Dodanie nowego handlera dla pliku logów
    logger.add(log_file, level=log_level, rotation=rotation, compression=compression, format=format)

    # Dodanie handlera dla terminala
    logger.add(sys.stderr, level=log_level, format=format)

    # Przekierowanie logowania standardowego do Loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord):
            try:
                level = logger.level(record.levelname).name
            except KeyError:
                level = record.levelno
            logger_opt = logger.opt(depth=6, exception=record.exc_info)
            logger_opt.log(level, record.getMessage())

    logging.basicConfig(handlers=[InterceptHandler()], level=log_level)

    # Informacja o zakończonej konfiguracji logowania
    logger.info("Logowanie zostało poprawnie skonfigurowane.")
