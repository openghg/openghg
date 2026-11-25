import xarray as xr
import pytest
from helpers import get_flux_datapath
from openghg.standardise.flux import parse_openghg
from pandas import Timestamp


def test_parse_openghg_multi_file():
    """
    Test file in OpenGHG format (variables and attributes) can be
    correctly parsed when multiple files are passed.
    """
    datapath_1 = get_flux_datapath(filename="co2-gpp-cardamom_EUROPE_2013.nc")
    datapath_2 = get_flux_datapath(filename="co2-gpp-cardamom_EUROPE_2012.nc")

    species = "co2"
    source = "anthro"
    domain = "EUROPE"

    filepath = [datapath_1, datapath_2]

    results = parse_openghg(
        filepath,
        domain=domain,
        species=species,
        source=source,
    )

    assert "co2_anthro_EUROPE" in results

    data = results["co2_anthro_EUROPE"]["data"]

    time = data["time"]
    assert time[0] == Timestamp("2012-01-01")
    assert time[-1] == Timestamp("2013-01-01")


def test_parse_openghg_data():
    """Direct flux input receives the same preprocessing as filepath input."""
    filepath = get_flux_datapath(filename="co2-gpp-cardamom_EUROPE_2013.nc")

    filepath_results = parse_openghg(
        filepath=filepath,
        domain="EUROPE",
        species="co2",
        source="anthro",
    )
    with xr.open_dataset(filepath) as dataset:
        results = parse_openghg(
            data=dataset.load(),
            domain="EUROPE",
            species="co2",
            source="anthro",
        )

    assert "co2_anthro_EUROPE" in results
    direct_data = results["co2_anthro_EUROPE"]["data"]
    filepath_data = filepath_results["co2_anthro_EUROPE"]["data"]
    xr.testing.assert_equal(direct_data, filepath_data)
    assert direct_data.time[0] == Timestamp("2013-01-01")


def test_parse_openghg_data_rejects_mismatched_known_domain():
    """Direct flux input validates coordinates against a known domain."""
    filepath = get_flux_datapath(filename="co2-gpp-cardamom_EUROPE_2013.nc")

    with xr.open_dataset(filepath) as dataset:
        with pytest.raises(ValueError, match="coordinates does not match"):
            parse_openghg(
                data=dataset.load(),
                domain="USA",
                species="co2",
                source="anthro",
            )
