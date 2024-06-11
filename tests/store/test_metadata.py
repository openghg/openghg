import pytest

from openghg.objectstore import create_default_config, get_metakey_defaults
from openghg.store.base._metadata import (
    Metadatum,
    Metadata,
    categorising_keys_valid,
    metadata_from_config,
    merge_dicts,
)


def test_metadatum_postinit():
    """Metadatum can only have `require = True` if `categorising = True`"""
    non_cat_metadatum = Metadatum(name="ncmd", value=None, categorising=False, required=True)

    assert non_cat_metadatum.required is False


def test_metadatum_valid_check():
    """Test `valid` method of Metadatum. It should only be False for Metadatum with
    `categorising = True` and `value = None`
    """
    valid_metadatum1 = Metadatum(name="vmd1", value=None, categorising=True, required=False)
    assert valid_metadatum1.valid

    valid_metadatum2 = Metadatum(name="vmd2", value="not none", categorising=True, required=True)
    assert valid_metadatum2.valid

    valid_metadatum3 = Metadatum(name="vmd3", value=None, categorising=False, required=False)
    assert valid_metadatum3.valid

    invalid_metadatum = Metadatum(name="invmd", value=None, categorising=True, required=True)
    assert not invalid_metadatum.valid


def test_metadatum_update_value():
    """Test updating the value of a Metadatum object via an explict value
    or via another Metadatum object. Check that a Metadatum object can only have
    its value updated by another Metadatum object if they have the same name, and
    test that other attributes are unchanged."""
    metadatum1 = Metadatum(name="md1", value=None)

    metadatum2 = metadatum1.update_value("val1")

    assert metadatum2 == Metadatum(name="md1", value="val1")

    metadatum3 = Metadatum(name="md1", value="val1", categorising=True)
    metadatum4 = metadatum1.update_value(metadatum3)

    assert metadatum4 == Metadatum(name="md1", value="val1")

    with pytest.raises(ValueError):
        metadatum1.update_value(Metadatum(name="md2", value=None))


def test_metadata_init():
    metadatum1 = Metadatum(name="md1", value=None)
    metadatum2 = Metadatum(name="md2", value=None)
    metadatum3 = Metadatum(name="md3", value=None)

    metadata = Metadata.from_list([metadatum1, metadatum2, metadatum3])

    assert ("md1" in metadata) and ("md2" in metadata) and ("md3" in metadata)


def test_metadata_get_set():
    metadata = Metadata()

    # test adding as with a dictionary
    metadata["species"] = "CH4"

    # we can retrieve as with a dictionary
    assert metadata["species"] == "CH4"

    # the .data attribute contains an actual dictionary with the Metadatum
    # in this case, the default values for 'categorising' and 'required' are used
    assert metadata.data["species"] == Metadatum(name="species", value="CH4")


def test_metadata_update():
    metadata = Metadata.from_list([Metadatum(name="species", value=None, categorising=True, required=True)])

    # currently not valid
    assert not metadata.data["species"].valid

    # update value so it is a valid required Metadatum
    metadata["species"] = "CH4"

    assert metadata.data["species"].valid
    assert metadata["species"] == "CH4"


def test_init_vs_from_list():
    metadatum1 = Metadatum(name="md1", value=1)
    metadatum2 = Metadatum(name="md2", value=2)
    metadatum3 = Metadatum(name="md3", value=3)

    metadata1 = Metadata.from_list([metadatum1, metadatum2, metadatum3])

    _dict = {"md1": 1, "md2": 2, "md3": 3}

    metadata2 = Metadata(_dict)

    assert metadata1 == metadata2

    metadatum_dict = {"md1": metadatum1, "md2": metadatum2, "md3": metadatum3}
    metadata3 = Metadata(metadatum_dict)

    assert metadata2 == metadata3


def test_from_config(tmp_path):
    create_default_config(bucket=str(tmp_path))
    flux_metakey_defaults = get_metakey_defaults()["flux"]

    metadata = metadata_from_config(bucket=str(tmp_path), data_type="flux")

    for key in flux_metakey_defaults["required"]:
        assert key in metadata

        metadatum = metadata.data[key]
        assert metadatum.categorising and metadatum.required

    for key in flux_metakey_defaults["optional"]:
        assert key in metadata

        metadatum = metadata.data[key]
        assert metadatum.categorising and not metadatum.required


def test_metadata_processing(tmp_path):
    """Test processing that combines user metadata"""
    create_default_config(bucket=str(tmp_path))

    config_metadata = metadata_from_config(bucket=str(tmp_path), data_type="flux")

    # set-up user inputs:
    # "required" inputs
    # "optonal" inputs (e.g. via kwargs in standardise)
    # "info" inputs (meant only for searching, not as categorising metadata)
    user_required = {"species": "ch4", "source": "edgar-annual-total", "domain": "europe"}
    user_optional = {"database": "edgar", "database_version": "v8.0"}
    user_info = {"project": "paris"}

    # combine user inputs
    user_metadata_dict = user_info.copy()
    user_metadata_dict.update(user_optional)
    user_metadata_dict.update(user_required)

    user_metadata = Metadata(user_metadata_dict)

    # update the config_metadata, which should fill all of the values
    # in the config with the user's inputs
    config_metadata.update(user_metadata)

    # all required values have been provided, so categorising metadata
    # should be valid
    assert categorising_keys_valid(config_metadata)

    # check values
    for k, v in config_metadata.items():
        # user provided all required and optional keys except "model"
        if k != "model":
            assert v == user_metadata[k]

    # "model" should not have been updated
    assert config_metadata["model"] is None

    # the "info" metadata should not be categorising
    assert config_metadata.data["project"].categorising is False

    # we can recover the user's combined metadata
    user_metadata_recovered = dict(config_metadata)
    del user_metadata_recovered["model"]

    assert user_metadata_recovered == user_metadata


def test_merge_dicts():
    dict1 = {"a": 1, "b": 2}
    dict2 = {"a": 5, "c": 3}

    # join = "union"
    assert merge_dicts(dict1, dict2, join="union", on_conflict="left") == {"a": 1, "b": 2, "c": 3}
    assert merge_dicts(dict1, dict2, join="union", on_conflict="right") == {"a": 5, "b": 2, "c": 3}
    assert merge_dicts(dict1, dict2, join="union", on_conflict="drop") == {"b": 2, "c": 3}

    with pytest.raises(ValueError):
        merge_dicts(dict1, dict2, join="union", on_conflict="error")

    # join = "intersection"
    assert merge_dicts(dict1, dict2, join="intersection", on_conflict="left") == {"a": 1}
    assert merge_dicts(dict1, dict2, join="intersection", on_conflict="right") == {"a": 5}
    assert merge_dicts(dict1, dict2, join="intersection", on_conflict="drop") == {}

    with pytest.raises(ValueError):
        merge_dicts(dict1, dict2, join="intersection", on_conflict="error")

    # join = "left"
    assert merge_dicts(dict1, dict2, join="left", on_conflict="left") == {"a": 1, "b": 2}
    assert merge_dicts(dict1, dict2, join="left", on_conflict="right") == {"a": 5, "b": 2}
    assert merge_dicts(dict1, dict2, join="left", on_conflict="drop") == {"b": 2}

    with pytest.raises(ValueError):
        merge_dicts(dict1, dict2, join="left", on_conflict="error")

    # join = "right"
    assert merge_dicts(dict1, dict2, join="right", on_conflict="left") == {"a": 1, "c": 3}
    assert merge_dicts(dict1, dict2, join="right", on_conflict="right") == {"a": 5, "c": 3}
    assert merge_dicts(dict1, dict2, join="right", on_conflict="drop") == {"c": 3}

    with pytest.raises(ValueError):
        merge_dicts(dict1, dict2, join="right", on_conflict="error")
