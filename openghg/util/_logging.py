import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

from rich.logging import RichHandler


def _get_logfile_path(default_log_dir: Path | None = None) -> Path:
    """Return the log file path for OpenGHG.

    Args:
        default_log_dir: optional default for logs. This is passed as an argument
            because the location of the openghg config dir is set in util._user.

    Returns:
        Path to log file.
    """
    env_path = os.environ.get("OPENGHG_LOG_PATH")
    if env_path:
        return Path(env_path).expanduser()

    default_log_dir = default_log_dir or Path.home()  # fall back to $HOME
    return default_log_dir / "openghg.log"


def _has_file_handler_for_path(logger: logging.Logger, logfile_path: Path) -> bool:
    """Return True if logger already has a file handler for logfile_path."""
    target_path = logfile_path.resolve()

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler_path = getattr(handler, "baseFilename", None)
            if handler_path is not None and Path(handler_path).resolve() == target_path:
                return True

    return False


def _has_rich_handler(logger: logging.Logger) -> bool:
    """Return True if logger already has a Rich console handler."""
    return any(isinstance(handler, RichHandler) for handler in logger.handlers)


def configure_logger(default_log_dir: Path | None = None) -> logging.Logger:
    """Configure and return the OpenGHG logger.

    This is safe to call multiple times. Repeated calls reuse the same named
    logger and avoid adding duplicate handlers.

    Args:
        default_log_dir: optional default for logs. This is passed as an argument
            because the location of the openghg config dir is set in util._user.

    Returns:
        Configured logger.
    """
    logger = logging.getLogger("openghg")
    logger.setLevel(logging.DEBUG)
    logging.captureWarnings(capture=True)

    logfile_path = _get_logfile_path(default_log_dir)
    logfile_path.parent.mkdir(parents=True, exist_ok=True)

    if not _has_file_handler_for_path(logger, logfile_path):
        file_handler = RotatingFileHandler(
            logfile_path,
            maxBytes=10 * 1024 * 1024,  # 10MiB limit
            backupCount=10,
            encoding="utf-8",
        )
        file_formatter = logging.Formatter(
            "%(asctime)s:%(levelname)s:%(name)s:%(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    if not _has_rich_handler(logger):
        console_handler = RichHandler()
        console_formatter = logging.Formatter(
            "%(levelname)s:%(name)s:%(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

    return logger
