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

    to_add = {"forgotten_key": "tis_but_a_scratch"}

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

    search_res.update_metadata(uid="test-uuid-101", updated_metadata=to_add)

    res = search_surface(site="tac", species="co2")

    expected_updated = start_metadata
    expected_updated["test-uuid-101"]["forgotten_key"] = "tis_but_a_scratch"

    assert res.metadata == expected_updated


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
    res.update_metadata(uid=uids, updated_metadata=to_add)

    search_res = search_surface(site="tac", inlet="100m")

    for metadata in search_res.metadata.values():
        assert metadata["data_owner"] == "michael palin"
        assert metadata["data_owner_email"] == "palin@python.com"


def test_invalid_uuid_raises():
    res = find_metadata(data_type="surface", site="tac")

    with pytest.raises(ValueError):
        res.update_metadata(uid="123-567", updated_metadata={})
