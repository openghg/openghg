from pathlib import Path
import pytest
import toml
from openghg.types import ConfigFileError
from openghg.util import check_config, create_config, read_local_config

@pytest.fixture
def tmp_config_path(tmp_path):
    return tmp_path.joinpath("config_folder").joinpath("mock_config.conf")


@pytest.fixture
def tmp_home_path(tmp_path):
    return tmp_path.joinpath("tmp_home_path_config_tests")


@pytest.fixture
def mock_get_user_config_path(tmp_config_path, mocker):
    tmp_config_path.parent.mkdir(parents=True)
    mocker.patch("openghg.util._user.get_user_config_path", return_value=tmp_config_path)


@pytest.fixture
def write_mock_config(tmpdir, tmp_config_path):
    mock_uuid = "179dcd5f-d5bb-439d-a3c2-9f690ac6d3b8"
    mock_path = Path(tmpdir).joinpath("mock_store")
    mock_shared_path = Path(tmpdir).joinpath("mock_shared_store")
    mock_conf = {
        "object_store": {
            "user": {"path": mock_path, "permissions": "rw"},
            "shared": {"path": mock_shared_path, "permission": "rw"},
        },
        "user_id": mock_uuid,
    }

    tmp_config_path.write_text(toml.dumps(mock_conf))


def test_read_config_check_old_stores(
    mock_get_user_config_path, write_mock_config, caplog
):
    """This tests the read_local_config function when the user has an old store in their config file.
    This test and the _check_valid_store function may be removed once the move to the new store setup is complete.
    """
    config = read_local_config()

    user_store_path = Path(config["object_store"]["user"]["path"])

    # Let's mock some data having been written to the object store
    zarr_store_folderpath = user_store_path.joinpath("data/test-uuid-123/zarr")
    zarr_store_folderpath.mkdir(parents=True)

    config = read_local_config()

    assert "Zarr storage format and will be ignored" not in caplog.text

    zarr_store_folderpath.rmdir()

    config = read_local_config()

    assert len(config["object_store"]) == 1
    assert "Zarr storage format and will be ignored" in caplog.text

    # Let's now make a zarr folder and make sure this object store is still valid
    shared_store_path = Path(config["object_store"]["shared"]["path"])
    zarr_store_folderpath = shared_store_path.joinpath("data/test-uuid-123/zarr")
    zarr_store_folderpath.mkdir(parents=True)

    config = read_local_config()
    assert len(config["object_store"]) == 1

    # Now we remove the zarr store and check the config again
    # As there's now a folder structure there a ConfigFileError should be raised
    # as the store is no longer valid and OpenGHG can't find a valid store
    zarr_store_folderpath.rmdir()

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
    mock_conf = {
        "object_store": {"user": {"path": mock_path, "permissions": "rw"}},
        "user_id": mock_uuid,
        "config_version": "2",
    }
    mocker.patch("openghg.util._user.read_local_config", return_value=mock_conf)

    mock_config_path.write_text("testing-123")

    check_config()

    assert " /tmp/mock_store does not exist but will be created." in caplog.text
