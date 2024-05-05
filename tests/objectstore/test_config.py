import pytest
from openghg.objectstore import get_metakey_defaults, create_default_config, get_metakeys, write_metakeys
from openghg.store import data_class_info


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

    assert (tmp_store / "config" / "metadata_keys.toml").exists()


def test_get_metakeys(tmp_store):
    create_default_config(bucket=tmp_store)

    metakeys = get_metakeys(bucket=tmp_store)

    assert metakeys.keys() == data_class_info().keys()


def test_write_metakeys(tmp_store):
    mock_keys = {"required": {"site": {"type": ["str"]}}}
    mock_metakeys = {k: mock_keys for k in data_class_info()}

    write_metakeys(bucket=tmp_store, metakeys=mock_metakeys)

    with pytest.raises(ValueError):
        mock_metakeys = {}
        write_metakeys(bucket=tmp_store, metakeys=mock_metakeys)
