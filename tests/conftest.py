import os
import sys
import tempfile
import shutil

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


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: slow")


def pytest_sessionstart(session):
    """Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    # Save the old OpenGHG object store environment variable if there is one
    old_path = os.environ.get("OPENGHG_PATH")

    if old_path is not None:
        os.environ["OPENGHG_PATH_BAK"] = old_path

    os.environ["OPENGHG_PATH"] = str(tempfile.TemporaryDirectory().name)


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished, right before
    returning the exit status to the system.
    """
    temp_path = os.environ["OPENGHG_PATH"]
    # Delete the testing object store
    try:
        shutil.rmtree(temp_path)
    except FileNotFoundError:
        pass

    # Set the environment variable back
    try:
        os.environ["OPENGHG_PATH"] = os.environ["OPENGHG_PATH_BAK"]
        del os.environ["OPENGHG_PATH_BAK"]
    except KeyError:
        pass
