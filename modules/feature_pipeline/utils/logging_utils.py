import time
from functools import wraps

from logger import logger


def format_logging(
    progress: str, stage: str = "Here", exists: bool = False, area_name: str = ""
) -> str:
    """
    Helper for log formatting
    Args:
        progress: text for sub-window or region progress
        stage: name of the stage of the pipeline
        exists: if the file exists or not
        area_name: name of the area

    Returns:
        formatted log text
    """
    if exists:
        return f"{stage} {area_name} -- already computed for {progress}."
    else:
        return f"{stage} {area_name} -- retrieved for {progress}."


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
