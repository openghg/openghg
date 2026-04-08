import logging
from pathlib import Path

import pytest
from rich.logging import RichHandler

import openghg.util._logging as logging_module


def _clear_openghg_logger() -> None:
    """Remove and close all handlers from the openghg logger."""
    logger = logging.getLogger("openghg")
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()


def _reload_logging_module():
    """Import and reload the logging module after environment changes."""
    logging_module.configure_logger()


def _get_file_handlers() -> list[logging.FileHandler]:
    """Return file handlers attached to the openghg logger."""
    logger = logging.getLogger("openghg")
    return [handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)]


def _get_rich_handlers() -> list[RichHandler]:
    """Return Rich handlers attached to the openghg logger."""
    logger = logging.getLogger("openghg")
    return [handler for handler in logger.handlers if isinstance(handler, RichHandler)]


@pytest.fixture(autouse=True)
def clean_logger_state():
    """Ensure tests do not leak logger handlers into each other."""
    _clear_openghg_logger()
    yield
    _clear_openghg_logger()


def test_logging_default_path_uses_home(monkeypatch, tmp_path):
    """Default log path should resolve to ~/openghg.log when no env var is set."""
    monkeypatch.delenv("OPENGHG_LOG_PATH", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))

    _reload_logging_module()

    file_handlers = _get_file_handlers()
    assert len(file_handlers) == 1
    assert Path(file_handlers[0].baseFilename) == tmp_path / "openghg.log"


def test_logging_env_override(monkeypatch, tmp_path):
    """OPENGHG_LOG_PATH should override the default log location."""
    log_path = tmp_path / "logs" / "custom.log"
    monkeypatch.setenv("OPENGHG_LOG_PATH", str(log_path))

    _reload_logging_module()

    file_handlers = _get_file_handlers()
    assert len(file_handlers) == 1
    assert Path(file_handlers[0].baseFilename) == log_path
    assert log_path.parent.exists()


def test_logging_adds_console_handler(monkeypatch, tmp_path):
    """A Rich console handler should be attached."""
    monkeypatch.setenv("OPENGHG_LOG_PATH", str(tmp_path / "openghg.log"))

    _reload_logging_module()

    rich_handlers = _get_rich_handlers()
    assert len(rich_handlers) == 1
    assert rich_handlers[0].level == logging.INFO


def test_logging_does_not_duplicate_handlers_on_reload(monkeypatch, tmp_path):
    """Repeated imports/reloads should not duplicate file or console handlers."""
    monkeypatch.setenv("OPENGHG_LOG_PATH", str(tmp_path / "openghg.log"))

    _reload_logging_module()
    logger = logging.getLogger("openghg")
    first_handler_count = len(logger.handlers)
    first_file_handler_count = len(_get_file_handlers())
    first_rich_handler_count = len(_get_rich_handlers())

    _reload_logging_module()
    logger = logging.getLogger("openghg")
    second_handler_count = len(logger.handlers)
    second_file_handler_count = len(_get_file_handlers())
    second_rich_handler_count = len(_get_rich_handlers())

    assert second_handler_count == first_handler_count
    assert second_file_handler_count == first_file_handler_count == 1
    assert second_rich_handler_count == first_rich_handler_count == 1


def test_logging_writes_message_to_file(monkeypatch, tmp_path):
    """Messages logged through the openghg logger should be written to the file."""
    log_path = tmp_path / "openghg.log"
    monkeypatch.setenv("OPENGHG_LOG_PATH", str(log_path))

    _reload_logging_module()

    logger = logging.getLogger("openghg")
    test_message = "test message from pytest"
    logger.info(test_message)

    for handler in logger.handlers:
        if hasattr(handler, "flush"):
            handler.flush()

    assert log_path.exists()
    contents = log_path.read_text(encoding="utf-8")
    assert test_message in contents


def test_logging_logger_level_is_debug(monkeypatch, tmp_path):
    """The openghg logger should be configured at DEBUG level."""
    monkeypatch.setenv("OPENGHG_LOG_PATH", str(tmp_path / "openghg.log"))

    _reload_logging_module()

    logger = logging.getLogger("openghg")
    assert logger.level == logging.DEBUG
