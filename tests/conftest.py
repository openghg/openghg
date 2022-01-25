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


@pytest.fixture(autouse=True, scope="session")
def set_envs():
    os.environ["ACQUIRE_HOST"] = "localhost:8080"
    os.environ["OPENGHG_PATH"] = temporary_store_path


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


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-cfchecks"):
        # --run-cfchecks given in cli: do not skip slow tests
        return

    skip_cf = pytest.mark.skip(reason="need --run-cfchecks option to run")
    for item in items:
        if "cfchecks" in item.keywords:
            item.add_marker(skip_cf)
