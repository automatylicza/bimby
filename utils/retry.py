import asyncio
import functools
from typing import Callable, Type, Tuple
import logging

def retry_async(
    exceptions: Tuple[Type[BaseException], ...],
    tries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    max_delay: float = 30.0,  # Dodano max_delay jako parametr
    logger: logging.Logger = None,
) -> Callable:
    """
    Dekorator do ponownego wykonywania asynchronicznych funkcji w przypadku wystąpienia określonych wyjątków.

    Dekorator próbuje ponownie wykonać funkcję asynchroniczną, jeśli zgłosi wyjątek zdefiniowany w `exceptions`.
    Liczba prób oraz czas oczekiwania między próbami są kontrolowane za pomocą parametrów `tries`, `delay`, `backoff` i `max_delay`.

    Args:
        exceptions (Tuple[Type[BaseException], ...]): Wyjątki, które wyzwalają ponowienie.
        tries (int): Liczba prób wykonania funkcji (domyślnie 3).
        delay (float): Początkowy czas oczekiwania między próbami w sekundach (domyślnie 1.0).
        backoff (float): Współczynnik zwiększania czasu oczekiwania między kolejnymi próbami (domyślnie 2.0).
        max_delay (float): Maksymalny czas oczekiwania między próbami w sekundach (domyślnie 30.0).
        logger (logging.Logger): Logger do użycia do logowania informacji o błędach (wymagane).

    Returns:
        Callable: Dekorowana funkcja.

    Raises:
        ValueError: Jeśli logger nie zostanie przekazany.
    """
    if not logger:
        raise ValueError("Logger is required for retry_async decorator.")

    def decorator_retry(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    msg = f"{func.__name__} failed with {e}, retrying in {mdelay:.2f} seconds... (Attempts left: {mtries - 1})"
                    logger.warning(msg)
                    # Oczekiwanie z uwzględnieniem limitu maksymalnego opóźnienia
                    await asyncio.sleep(min(mdelay, max_delay))
                    mtries -= 1
                    mdelay = min(mdelay * backoff, max_delay)
            # Ostatnia próba
            try:
                return await func(*args, **kwargs)
            except exceptions as e:
                logger.error(f"{func.__name__} failed on the last attempt: {e}")
                raise  # Propaguj wyjątek dalej
        return wrapper_retry
    return decorator_retry
