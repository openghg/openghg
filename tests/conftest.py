import os
import sys
import tempfile
import shutil
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
    global temporary_store
    monkeypatch.setenv("OPENGHG_STORE", temporary_store_path)
    monkeypatch.setenv("ACQUIRE_HOST", "localhost:8080")

# def pytest_configure(config):
#     config.addinivalue_line("markers", "slow: slow")


# def pytest_sessionstart(session):
#     """Called after the Session object has been created and
#     before performing collection and entering the run test loop.
#     """
#     # Save the old OpenGHG object store environment variable if there is one
#     old_path = os.environ.get("OPENGHG_PATH")

#     if old_path is not None:
#         os.environ["OPENGHG_PATH_BAK"] = old_path

#     os.environ["OPENGHG_PATH"] = str(tempfile.TemporaryDirectory().name)


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished, right before
    returning the exit status to the system.
    """
    global temporary_store
    temporary_store.cleanup()
    # temp_path = os.environ["OPENGHG_PA TH"]
    # # Delete the testing object store
    # try:
    #     shutil.rmtree(temp_path)
    # except FileNotFoundError:
    #     pass

    # # Set the environment variable back
    # try:
    #     # Delete the testing object store
    #     shutil.rmtree(temp_path)
    # except FileNotFoundError:
    #     pass

    # try:
    #     os.environ["OPENGHG_PATH"] = os.environ["OPENGHG_PATH_BAK"]
    #     del os.environ["OPENGHG_PATH_BAK"]
    # except KeyError:
    #     pass
