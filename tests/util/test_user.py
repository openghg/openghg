from openghg.util import create_config, check_config, read_local_config
import toml
from pathlib import Path
import pytest
from openghg.types import ConfigFileError


@pytest.fixture
def mock_conf_path(tmpdir, monkeypatch, mocker):
    monkeypatch.setenv("HOME", str(tmpdir))
    mock_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_path)


@pytest.fixture
def mock_contents():
    mock_uuid = "179dcd5f-d5bb-439d-a3c2-9f690ac6d3b8"
    mock_path = "/tmp/mock_store"
    return {"object_store": {"user": {"path": mock_path, "permissions": "rw"}, "user_id": mock_uuid}}


def check_read_local_config():
    # Make sure we can't read an empty config
    with pytest.raises(ConfigFileError):
        read_local_config()


def test_create_config(monkeypatch, mocker, tmpdir):
    monkeypatch.setenv("HOME", str(tmpdir))
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")

    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)
    mock_uuids = [f"test-uuid-{x}" for x in range(100, 110)]
    mocker.patch("openghg.util._user.uuid.uuid4", side_effect=mock_uuids)

    create_config(silent=True)

    user_obj_expected = Path.home().joinpath("openghg_store").absolute()

    config = toml.loads(mock_config_path.read_text())

    assert config["user_id"] == "test-uuid-100"
    assert config["object_store"]["user"]["path"] == str(user_obj_expected)
    assert config["object_store"]["user"]["permissions"] == "rw"


def test_create_config_migrate(mocker, monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("HOME", str(tmp_path))

    # make config file at tmp_path/.config/openghg/openghg.conf
    mock_old_config_path = tmp_path / ".config" / "openghg" / "openghg.conf"
    mock_old_config_path.parent.mkdir(parents=True)

    mock_config_content = "This is just a mock config file."
    mock_old_config_path.write_text(mock_config_content)

    mock_new_config_path = tmp_path / ".openghg" / "openghg.conf"
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_new_config_path)

    create_config(silent=True)

    assert "Moved user config file from" in caplog.text
    assert "Cannot update an existing configuration silently. Please run interactively." in caplog.text
    assert mock_new_config_path.read_text() == mock_config_content


def test_check_config(mocker, caplog, monkeypatch, tmpdir):
    monkeypatch.setenv("HOME", str(tmpdir))
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")

    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)

    with pytest.raises(ConfigFileError):
        check_config()

    mock_uuid = "179dcd5f-d5bb-439d-a3c2-9f690ac6d3b8"
    mock_path = "/tmp/mock_store"
    mock_conf = {"object_store": {"user": {"path": mock_path, "permissions": "rw"}}, "user_id": mock_uuid, "config_version": "2"}
    mocker.patch("openghg.util._user.read_local_config", return_value=mock_conf)

    mock_config_path.write_text("testing-123")

    check_config()

    assert " /tmp/mock_store does not exist but will be created." in caplog.text
