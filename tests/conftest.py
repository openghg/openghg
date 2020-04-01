
import pytest
import sys
import os

# load all of the common fixtures used by the mocked tests
pytest_plugins = ["mock.fixtures.mocked_services"]

# Added for import of services modules in tests
sys.path.insert(0, os.path.abspath("services"))

# Added for import of hugs from testing directory
sys.path.insert(0, os.path.abspath("."))

def pytest_configure(config):
    config.addinivalue_line("markers", "slow: slow")

acquire_dir = "../acquire"

# sys.path.insert(0, os.path.abspath(acquire_dir))
sys.path.insert(0, os.path.abspath(f"{acquire_dir}/services"))

