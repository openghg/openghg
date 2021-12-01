import os
import sys
import tempfile
import pytest

# Added for import of services modules in tests
sys.path.insert(0, os.path.abspath("services"))

# Added for import of openghg from testing directory
sys.path.insert(0, os.path.abspath("."))

# We still require OpenGHG to be in the directory above OpenGHG when running the tests
acquire_dir = "../acquire"

# Use the local Acquire
sys.path.insert(0, os.path.abspath(acquire_dir))
sys.path.insert(0, os.path.abspath(f"{acquire_dir}/services"))

# load all of the common fixtures used by the mocked tests
pytest_plugins = ["services.fixtures.mocked_services"]

temporary_store = tempfile.TemporaryDirectory()
temporary_store_path = temporary_store.name


@pytest.fixture(autouse=True)
def set_envs(monkeypatch):
    monkeypatch.setenv("OPENGHG_STORE", temporary_store_path)
    monkeypatch.setenv("ACQUIRE_HOST", "localhost:8080")


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished, right before
    returning the exit status to the system.
    """
    print(f"\n\nCleaning up testing store at {temporary_store.name}")
    temporary_store.cleanup()
