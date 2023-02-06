import os
import sys
import tempfile

import pytest

# Added for import of openghg from testing directory
sys.path.insert(0, os.path.abspath("."))

temporary_store = tempfile.TemporaryDirectory()
temporary_store_path = temporary_store.name


# @pytest.fixture(autouse=True)
# def mock_auth_fixture(monkeypatch):
#     def return_mock():
#         return {
#             "object_store": {"local_store": str(temporary_store_path)},
#             "user_id": "test-id-123",
#         }

#     monkeypatch.setattr("openghg.util.read_local_config", return_mock)

from typing import Iterator
from unittest.mock import patch


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture() -> Iterator[None]:
    mock_config = {
        "object_store": {"local_store": str(temporary_store_path)},
        "user_id": "test-id-123",
    }
    with patch("openghg.util.read_local_config", return_value=mock_config):
        yield


def pytest_sessionstart(session):
    """Set the required environment variables for OpenGHG
    at the start of the test session.
    """
    os.environ["OPENGHG_TEST"] = temporary_store_path


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished, right before
    returning the exit status to the system.
    """
    print(f"\n\nCleaning up testing store at {temporary_store.name}")
    temporary_store.cleanup()


def pytest_addoption(parser):
    parser.addoption("--run-cfchecks", action="store_true", default=False, help="run CF compliance tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "cfchecks: mark mark test as needing CF related libs to run")
    config.addinivalue_line(
        "markers",
        "skip_if_no_cfchecker: skip test is cfchecker is not installed",
    )


def pytest_collection_modifyitems(config, items):
    messge_str = "need --run-cfchecks option to run"

    try:
        import cfchecker  # noqa

        cfchecker_imported = True
    except (FileNotFoundError, ImportError) as e:
        cfchecker_imported = False
        messge_str = f"Cannot import CFChecker - {e}"

    if config.getoption("--run-cfchecks") and cfchecker_imported:
        # --run-cfchecks given in cli: do not skip cfchecks tests
        return

    skip_cf = pytest.mark.skip(reason=messge_str)
    for item in items:
        if "cfchecks" in item.keywords:
            item.add_marker(skip_cf)
