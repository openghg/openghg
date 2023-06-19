from openghg.util import create_config, get_user_id, check_config
from openghg.util._user import migrate_config
import toml
from pathlib import Path
import os
import pytest


@pytest.fixture
def mock_config(mocker, scope="module"):
    mock_path = str(Path().home().joinpath("openghg_store"))
    mock_conf = {"object_store": {"local_store": mock_path}, "user_id": "test-uuid-100"}
    mocker.patch("openghg.util._user.read_local_config", return_value=mock_conf)


def test_get_user_id(mock_config):
    user_id = get_user_id()
    assert user_id == "test-uuid-100"


def test_create_config(mocker, tmpdir):
    mock_config_path = Path(tmpdir).joinpath("mock_config.conf")
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_config_path)
    mock_uuids = [f"test-uuid-{x}" for x in range(100, 110)]
    mocker.patch("openghg.util._user.uuid.uuid4", side_effect=mock_uuids)

    create_config(silent=True)

    user_obj_expected = Path.home().joinpath("openghg_store").absolute()

    config = toml.loads(mock_config_path.read_text())

    assert config["user_id"] == "test-uuid-100"
    assert config["object_store"]["local_store"] == str(user_obj_expected)


def test_check_config(mock_config, mocker, caplog):
    with pytest.raises(ValueError):
        check_config()

    mock_uuid = "179dcd5f-d5bb-439d-a3c2-9f690ac6d3b8"
    mock_path = "/tmp/mock_store"
    mock_conf = {"object_store": {"local_store": mock_path}, "user_id": mock_uuid}
    mocker.patch("openghg.util._user.read_local_config", return_value=mock_conf)

    check_config()

    assert " /tmp/mock_store does not exist but will be created." in caplog.text


def test_migrate_config_success(mocker, monkeypatch, tmp_path):
    monkeypatch.setitem(os.environ, 'HOME', str(tmp_path))

    # make config file at tmp_path/.config/openghg/openghg.conf
    mock_old_config_path = tmp_path / ".config" / "openghg" / "openghg.conf"
    mock_old_config_path.parent.mkdir(parents=True)
    mock_config_content = "This is just a mock config file."
    mock_old_config_path.write_text(mock_config_content)

    mock_new_config_path = tmp_path / ".ghgconfig" / "openghg" / "openghg.conf"
    mocker.patch("openghg.util._user.get_user_config_path", return_value=mock_new_config_path)

    migrate_config()

    assert not mock_old_config_path.exists()
    assert not mock_old_config_path.parent.exists() # openghg dir deleted
    assert mock_old_config_path.parent.parent.exists() # don't delete .config
    assert mock_new_config_path.exists()
    assert mock_new_config_path.read_text() == mock_config_content


def test_migrate_config_fail(monkeypatch, tmp_path):
    monkeypatch.setitem(os.environ, 'HOME', str(tmp_path))

    with pytest.raises(FileNotFoundError):
        migrate_config()
