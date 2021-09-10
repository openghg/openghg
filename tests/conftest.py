import os
import sys

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
