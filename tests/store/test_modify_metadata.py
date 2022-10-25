from openghg.store import find_metadata, ObsSurface
from openghg.retrieve import search_surface

import pytest
from helpers import get_surface_datapath, clear_test_store


@pytest.fixture(autouse=True)
def add_data(mocker):
    clear_test_store()
    mock_uuids = [f"test-uuid-{n}" for n in range(100, 150)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")

    ObsSurface.read_file(filepath=one_min, site="tac", network="decc", source_format="CRDS")


def test_find_modify_metadata():
    search_res = find_metadata(data_type="surface", site="tac", species="co2")

    to_add = {"forgotten_key": "tis_but_a_scratch", "another_key": "swallow", "a_third": "parrot"}

    start_metadata = {
        "test-uuid-101": {
            "site": "tac",
            "instrument": "picarro",
            "sampling_period": "60.0",
            "inlet": "100m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "co2",
            "calibration_scale": "wmo-x2007",
            "long_name": "tacolneston",
            "data_type": "surface",
            "inlet_height_magl": "100m",
            "data_owner": "simon o'doherty",
            "data_owner_email": "s.odoherty@bristol.ac.uk",
            "station_longitude": 1.13872,
            "station_latitude": 52.51775,
            "station_long_name": "tacolneston tower, uk",
            "station_height_masl": 50.0,
            "uuid": "test-uuid-101",
        }
    }

    search_res.update_metadata(uid="test-uuid-101", to_update=to_add)

    res = search_surface(site="tac", species="co2")

    diff_d = {
        k: v for k, v in res.metadata["test-uuid-101"].items() if k not in start_metadata["test-uuid-101"]
    }

    assert diff_d == {"forgotten_key": "tis_but_a_scratch", "another_key": "swallow", "a_third": "parrot"}


def test_modify_multiple_uuids():
    res = find_metadata(data_type="surface", site="tac")

    expected = {
        "test-uuid-100": {
            "site": "tac",
            "instrument": "picarro",
            "sampling_period": "60.0",
            "inlet": "100m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "ch4",
            "calibration_scale": "wmo-x2004a",
            "long_name": "tacolneston",
            "data_type": "surface",
            "inlet_height_magl": "100m",
            "data_owner": "simon o'doherty",
            "data_owner_email": "s.odoherty@bristol.ac.uk",
            "station_longitude": 1.13872,
            "station_latitude": 52.51775,
            "station_long_name": "tacolneston tower, uk",
            "station_height_masl": 50.0,
            "uuid": "test-uuid-100",
        },
        "test-uuid-101": {
            "site": "tac",
            "instrument": "picarro",
            "sampling_period": "60.0",
            "inlet": "100m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "co2",
            "calibration_scale": "wmo-x2007",
            "long_name": "tacolneston",
            "data_type": "surface",
            "inlet_height_magl": "100m",
            "data_owner": "simon o'doherty",
            "data_owner_email": "s.odoherty@bristol.ac.uk",
            "station_longitude": 1.13872,
            "station_latitude": 52.51775,
            "station_long_name": "tacolneston tower, uk",
            "station_height_masl": 50.0,
            "uuid": "test-uuid-101",
        },
    }

    assert res.metadata == expected

    uids = list(res.metadata.keys())
    to_add = {"data_owner": "michael palin", "data_owner_email": "palin@python.com"}
    res.update_metadata(uid=uids, to_update=to_add)

    search_res = search_surface(site="tac", inlet="100m")

    for metadata in search_res.metadata.values():
        assert metadata["data_owner"] == "michael palin"
        assert metadata["data_owner_email"] == "palin@python.com"


def test_invalid_uuid_raises():
    res = find_metadata(data_type="surface", site="tac")

    with pytest.raises(ValueError):
        res.update_metadata(uid="123-567", to_update={})


def test_delete_keys():
    res = find_metadata(data_type="surface", site="tac", species="ch4", inlet="100m")

    print(res.metadata)

    # search_res = search_surface(site="tac", inlet="100m", species="ch4")

    # print(search_res.metadata)

    # res = find_metadata(data_type="surface", site="tac", species="ch4", inlet="100m")

    res.update_metadata(uid="test-uuid-100", to_delete=["species"])

    return

    res = find_metadata(data_type="surface", site="tac", species="ch4", inlet="100m")

    # print(res.metadata)


def test_delete_and_modify_keys():
    res = find_metadata(data_type="surface", site="tac", species="ch4", inlet="100m")

    to_delete = ["station_longitude", "station_latitude"]

    res.update_metadata(uid="test-uuid-100", to_delete=to_delete)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    fresh_metadata = search_res.metadata["test-uuid-100"]

    assert "station_longitude" not in fresh_metadata
    assert "station_latitide" not in fresh_metadata

    res = find_metadata(data_type="surface", site="tac", species="ch4")

    to_update = {"sampling_period": "12H", "tasty_dish": "pasta"}

    # We've already deleted these keys
    with pytest.raises(KeyError):
        res.update_metadata(uid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    to_delete = ["long_name"]

    res.update_metadata(uid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    freshest_metadata = search_res.metadata["test-uuid-100"]

    assert "long_name" not in freshest_metadata

    assert freshest_metadata["sampling_period"] == "12H"
    assert freshest_metadata["tasty_dish"] == "pasta"


def test_try_delete_none_modify_none_changes_nothing():
    res = find_metadata(data_type="surface", site="tac", inlet="100m", species="ch4")

    res.update_metadata(uid="test-uuid-100")

    res.update_metadata(uid="test-uuid-100", to_update={}, to_delete=[])

    res2 = find_metadata(data_type="surface", site="tac", inlet="100m", species="ch4")

    assert res.metadata == res2.metadata
