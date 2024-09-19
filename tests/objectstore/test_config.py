import pytest
from openghg.objectstore import (
    check_metakeys,
    get_metakey_defaults,
    create_default_config,
    get_metakeys,
    write_metakeys,
)
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


def test_create_default_config(tmp_store):

    create_default_config(bucket=str(tmp_store))

    assert (tmp_store / "config" / "metadata_keys.json").exists()


def test_get_metakeys(tmp_store):
    create_default_config(bucket=tmp_store)

    metakeys = get_metakeys(bucket=tmp_store)

    assert metakeys.keys() == data_class_info().keys()


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
