import pytest
import json
from openghg.store import (
    check_metakeys,
    get_metakey_defaults,
    create_custom_config,
    get_metakeys,
    write_metakeys,
)
from openghg.store._metakeys_config import _get_custom_metakeys_filepath, _get_metakeys_from_file
from openghg.store import data_class_info
from openghg.types import ConfigFileError


@pytest.fixture
def tmp_store(tmp_path):
    return tmp_path / "testing_store_config"


def test_get_metakey_defaults():
    """Test to make sure the defaults file has a data type
    for each of the registered data types
    """
    defaults = get_metakey_defaults()
    storage_classes = data_class_info()

    assert defaults.keys() == storage_classes.keys()


def test_create_custom_config(tmp_store):
    """Test custom config can be created and detected in the object store."""
    create_custom_config(bucket=str(tmp_store))

    assert (tmp_store / "config" / "metadata_keys_v2.json").exists()


def test_get_metakeys():
    """Test metakeys can be extracted."""
    metakeys = get_metakeys()

    assert metakeys.keys() == data_class_info().keys()


def test_custom_metakeys(tmp_store):
    """Check metakeys can be read from a custom file"""
    create_custom_config(tmp_store)

    metakeys_filepath = _get_custom_metakeys_filepath(tmp_store)
    config_data = _get_metakeys_from_file(metakeys_filepath)

    new_key = "new_key"
    config_data["surface"]["required"][new_key] = {"type": ["str"]}

    with open(metakeys_filepath, "w") as filepath:
        json.dump(config_data, filepath)

    metakeys = get_metakeys(bucket=tmp_store)
    assert new_key in metakeys["surface"]["required"]


def test_write_metakeys(tmp_store):
    mock_keys = {"required": {"site": {"type": ["str"]}}}
    mock_metakeys = {k: mock_keys for k in data_class_info()}

    write_metakeys(bucket=tmp_store, metakeys=mock_metakeys)

    with pytest.raises(ConfigFileError):
        mock_metakeys = {}
        write_metakeys(bucket=tmp_store, metakeys=mock_metakeys)


def test_check_metakeys(caplog):
    mock_keys = {"required": {"site": {"type": ["str"]}}}
    correct = {k: mock_keys for k in data_class_info()}

    assert check_metakeys(metakeys=correct)

    incorrect = {k: mock_keys for k in ["footprints", "flux"]}

    assert not check_metakeys(incorrect)
