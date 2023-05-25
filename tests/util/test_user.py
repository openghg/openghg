from openghg.util import create_config, check_config
import toml
from pathlib import Path
import pytest
import logging


@pytest.fixture(scope="module")
def mock_conf_path(mocker, tmpdir):
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)


def test_create_config_no_exisiting(mocker, tmpdir):
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)
    mock_uuids = [f"test-uuid-{x}" for x in range(100, 110)]
    mocker.patch("openghg.util._user.uuid.uuid4", side_effect=mock_uuids)

    create_config(silent=True)

    user_obj_expected = Path.home().joinpath("openghg_store").absolute()

    config = toml.loads(mock_config_path.read_text())

    assert config["user_id"] == "test-uuid-100"
    assert config["config_version"] == "2"
    assert config["object_store"]["user"]["path"] == str(user_obj_expected)
    assert config["object_store"]["user"]["permissions"] == "rw"


def test_create_config_existing_file(mocker, tmpdir, caplog):
    caplog.set_level(logging.DEBUG, logger="openghg.util")

    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)

    create_config(silent=True)

    assert "Error: cannot overwrite an existing configuration. Please run interactively." not in caplog.text

    create_config(silent=True)

    assert "Error: cannot overwrite an existing configuration. Please run interactively." in caplog.text


def test_check_config_valid(tmpdir, mocker, caplog):
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)

    create_config(silent=True)
    assert check_config() is True
