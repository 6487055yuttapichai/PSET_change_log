import sys
import inspect
from loguru import logger

LOG_LEVEL = 'DEBUG'


def setup_logging():
    logger.remove()
    logger.add(sys.stderr,
               level=LOG_LEVEL,
               backtrace=True,
               diagnose=True,
               enqueue=True)
    logger.add("./tdm-reporter.log",
               rotation="00:00",
               retention="14 days",
               level=LOG_LEVEL,
               compression="gz",
               backtrace=True,
               diagnose=True,
               enqueue=True, )


setup_logging()


def class_method_name() -> str:
    # Get the current frame
    current_frame = inspect.currentframe()
    # Get the caller's frame (one level up from the current frame)
    caller_frame = current_frame.f_back

    # Retrieve the method name from the caller's frame
    method_name = caller_frame.f_code.co_name

    # Retrieve the class name if the caller's context is inside a class
    # Check if 'self' is in the caller's local variables
    class_name = caller_frame.f_locals.get('self', None).__class__.__name__ if 'self' in caller_frame.f_locals else None

    return f"{class_name}.{method_name}"

def log_error(mod: str, err_type: str, err_msg: str):
    logger.exception(err_msg) if LOG_LEVEL == 'DEBUG' else (
        logger.error(f"| {mod} | {err_type}: {err_msg}"))
