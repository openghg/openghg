import pytest
from helpers import get_eulerian_datapath, clear_test_store
from openghg.retrieve import search
from openghg.standardise import standardise_eulerian
from xarray import open_dataset


def test_read_file():
    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = standardise_eulerian(store="user", filepath=test_datapath, model="GEOSChem", species="ch4")

    assert "geoschem_ch4_2015-01-01" in proc_results

    search_results = search(
        species="ch4", model="geoschem", start_date="2015-01-01", data_type="eulerian_model"
    )

    euler_obs = search_results.retrieve_all()

    assert euler_obs

    eulerian_data = euler_obs.data
    metadata = euler_obs.metadata

    orig_data = open_dataset(test_datapath)

    assert orig_data["lat"].equals(eulerian_data["lat"])
    assert orig_data["lon"].equals(eulerian_data["lon"])
    assert orig_data["time"].equals(eulerian_data["time"])
    assert orig_data["lev"].equals(eulerian_data["lev"])
    assert orig_data["SpeciesConc_CH4"].equals(eulerian_data["SpeciesConc_CH4"])

    # TODO: Update Eulerian model input to run through same time recognition as
    # other similiar data types. Add period as input.

    expected_metadata_values = {
        "species": "ch4",
        "date": "2015-01-01",
        "start_date": "2015-01-16 12:00:00+00:00",
        "end_date": "2015-01-16 12:00:01+00:00",  # Update as appropriate.
        "max_longitude": 175.0,
        "min_longitude": -180.0,
        "max_latitude": 89.0,
        "min_latitude": -89.0,
    }

    for key, expected_value in expected_metadata_values.items():
        assert metadata[key] == expected_value


def test_optional_metadata_raise_error():
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """

    clear_test_store("user")
    with pytest.raises(ValueError):
        test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

        proc_results = standardise_eulerian(store="user", filepath=test_datapath, model="GEOSChem", species="ch4", optional_metadata={"species":"ch4", "tag":"tests"})


def test_optional_metadata():
    """
    Test to verify optional metadata supplied as dictionary gets stored as metadata
    """
    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = standardise_eulerian(store="user", filepath=test_datapath, model="GEOSChem", species="ch4", optional_metadata={"project":"openghg_tests", "tag":"tests"})

    search_results = search(
        species="ch4", model="geoschem", start_date="2015-01-01", data_type="eulerian_model"
    )

    euler_obs = search_results.retrieve_all()
    metadata = euler_obs.metadata

    assert "project" in metadata
    assert "tag" in metadata
