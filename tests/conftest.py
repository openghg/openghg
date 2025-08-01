import getpass
import tempfile
from typing import Iterator
import toml

from unittest.mock import patch
from pathlib import Path

import pytest
from helpers import clear_test_stores, get_info_datapath, temporary_store_paths


tmp_store_paths = temporary_store_paths()

from openghg.store import get_metakey_defaults

@pytest.fixture(scope="session", autouse=True)
def mock_configuration_paths() -> dict:
    return {
        "object_store": {
            "user": {"path": str(tmp_store_paths["user"]), "permissions": "rw"},
            "group": {"path": str(tmp_store_paths["group"]), "permissions": "rw"},
            "shared": {"path": str(tmp_store_paths["shared"]), "permissions": "r"},
        },
        "user_id": "test-id-123",
        "config_version": "2",
    }

@pytest.fixture(scope="session", autouse=True)
def default_session_fixture(mock_configuration_paths)-> Iterator[None]:
    with patch("openghg.objectstore._local_store.read_local_config", return_value=mock_configuration_paths):
        yield
        
@pytest.fixture(scope="session", autouse=True)
def mock_user_config(mock_configuration_paths):
    user = getpass.getuser() 

    temp_config = Path(tempfile.gettempdir())/ f"{user}" / "openghg.conf"
    mock_test_config_path = temp_config

    initial_config = mock_configuration_paths

    mock_test_config_path.parent.mkdir(parents=True, exist_ok=True)
    mock_test_config_path.write_text(toml.dumps(initial_config))

    with patch("openghg.util._user.get_user_config_path", return_value=mock_test_config_path):
        yield

@pytest.fixture(scope="function")
def reset_mock_user_config(mock_configuration_paths):
    user = getpass.getuser() 
    initial_config = mock_configuration_paths

    temp_config = Path(tempfile.gettempdir())/ f"{user}" / "openghg.conf"
    mock_test_config_path = temp_config
    mock_test_config_path.write_text("")
    mock_test_config_path.write_text(toml.dumps(initial_config))


@pytest.fixture(scope="session", autouse=True)
def mock_metakeys():
    # TODO - implement this in a different way
    default_keys = get_metakey_defaults()

    with patch("openghg.store.base._base.get_metakeys", return_value=default_keys):
        yield


@pytest.fixture(scope="session", autouse=True)
def openghg_defs_mock(session_mocker):
    """
    Mock the external call to openghg_defs module for site_info_file
    and species_info_file to replace this with a static version within the
    tests data directory.
    """
    import openghg_defs

    site_info_file = get_info_datapath(filename="site_info.json")
    session_mocker.patch.object(openghg_defs, "site_info_file", new=site_info_file)

    species_info_file = get_info_datapath(filename="species_info.json")
    session_mocker.patch.object(openghg_defs, "species_info_file", new=species_info_file)

    # TODO: Add domain_info as well?

    yield


def pytest_sessionstart(session):
    """Set the required environment variables for OpenGHG
    at the start of the test session.
    """
    clear_test_stores()


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished, right before
    returning the exit status to the system.
    """
    print("\n\nCleaning up testing stores.")
    clear_test_stores()


def pytest_addoption(parser):
    parser.addoption("--run-cfchecks", action="store_true", default=False, help="run CF compliance tests")
    parser.addoption("--run-icos", action="store_true", default=False, help="run ICOS tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "cfchecks: mark test as needing CF related libs to run")
    config.addinivalue_line(
        "markers",
        "skip_if_no_cfchecker: skip test is cfchecker is not installed",
    )
    config.addinivalue_line("markers", "icos: retrieve data from ICOS online portal")


def pytest_collection_modifyitems(config, items):
    cf_msg = "Pass --run-cfchecks to run CF compliance tests"

    run_cftests = config.getoption("--run-cfchecks")
    if run_cftests:
        try:
            import cfchecker  # noqa
        except ImportError:
            cf_msg = "cfchecker not installed, skipping CF compliance tests"
            run_cftests = False

    # TODO - could tidy this up with the user of item.iter_markers to
    # check for the actual marker instead of the string match in the name
    run_icos_tests = config.getoption("--run-icos")
    for item in items:
        if not run_cftests and "cfchecks" in item.keywords:
            item.add_marker(pytest.mark.skip(reason=cf_msg))

        if not run_icos_tests and "icos" in item.keywords:
            markers = [mark for mark in item.iter_markers() if mark.name == "icos"]
            if markers:
                item.add_marker(pytest.mark.skip(reason="Requires --run-icos option to run"))
