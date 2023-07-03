from typing import Union, Optional
from pathlib import Path
import logging

__all__ = ["add_file_handler", "add_stream_handler", "remove_stream_handler"]


def add_file_handler(logger: logging.Logger, logfile_path: Union[Path, str]) -> None:
    """
    Add FileHandler to a logger object for writing to a log file.
    Args:
        logger: Python Logger object
        logfile_path: Filepath for log file
    Returns:
        None
    """

    # Create file handler for log file - set to DEBUG (maximum detail)
    fileHandler = logging.FileHandler(logfile_path)  # May want to update this to user area
    fileFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    fileHandler.setFormatter(fileFormatter)
    fileHandler.setLevel(logging.DEBUG)
    logger.addHandler(fileHandler)


def add_stream_handler(logger: logging.Logger, check_exists: bool = False) -> None:
    """
    Add StreamHandler to a logger object to use for writing to the console.
    Args:
        logger: Python Logger object
    Returns:
        None
    """
    # Create console handler - set to WARNING (lower level)
    consoleHandler = logging.StreamHandler()
    consoleFormatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
    consoleHandler.setFormatter(consoleFormatter)
    consoleHandler.setLevel(logging.WARNING)

    add_handler = True
    if check_exists:
        if _find_stream_handler(logger) is not None:
            add_handler = False

    if add_handler:
        logger.addHandler(consoleHandler)
    else:
        print("StreamHandler already exists on logger object.")


def _find_stream_handler(logger: logging.Logger) -> Optional[logging.StreamHandler]:
    """
    If present, find the (first) StreamHandler attached to a logger object.
    Args:
        logger: Python Logger object
    Returns:
        StreamHandler: if present
        None: if no StreamHandler is present
    """

    handlers = logger.handlers
    for handler in handlers:
        if type(handler) is logging.StreamHandler:
            return handler

    return None


def remove_stream_handler(logger: logging.Logger) -> None:
    """
    Remove existing StreamHandler attached to a Logger object.
    Note: this will only remove the first instance found if multiple
    are present.
    Args:
        logger: Python Logger object
    Returns:
        None
    """
    stream_handler = _find_stream_handler(logger)
    if stream_handler is not None:
        logger.removeHandler(stream_handler)
    else:
        print("No StreamHandler on logger object to remove")
