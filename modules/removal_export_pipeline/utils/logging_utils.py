import time
from functools import wraps

from logger import logger


def log_func(func):
    """Decorator for logging the start and the end of a function"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        action_name = func.__name__
        logger.info(f"Started {action_name} phase...")
        start = time.time()
        result = func(self, *args, **kwargs)
        end = time.time()
        logger.info(f"Finished {action_name} phase in {end - start} seconds!")
        return result

    return wrapper
