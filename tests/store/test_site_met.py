import pytest
from openghg.util import clean_string
from openghg.standardise import standardise_site_met
from openghg.retrieve import search_site_met
from helpers import get_met_datapath

@pytest.mark.parametrize(
    "site,network,filename",
    [
        (
            "mhd",
            "agage",
            "Met_mhd_agage_201608.nc",
        ),
        (
            "tac",
            "agage",
            "Met_tac_agage_201608.nc",
        )]
)

def test_ecmwf(site, network, filename):
    """
    Test that downloaded Met ECMWF data can be added and retrieved from an object store
    using `standardise_site_met`.
    """

    met_filepath = get_met_datapath(filename)

    standardise_site_met(met_filepath,
                         site,
                         network,
                         source_format="ecmwf",
                         store="user")

    # Note if met_source is not specified by the user the formatted value of "ECMWF ERA5"
    # will be added by openghg.standardise.met.parse_ecmwf function
    met_source = "ECMWF ERA5"

    search_results = search_site_met(site=site, network=network, met_source=met_source)
    
    met_data = search_results.retrieve()
    dataset = met_data.data

    # test dataset

    assert dataset is not None


    expected_dims = ["time", "lat", "lon", "pressure_level", "inlet_height"]

    expected_vars = ["u_wind", "v_wind"]

    existing_dims = dataset.variables.keys()
    missing_dims = [var for var in expected_dims if var not in existing_dims]
    assert not missing_dims, f"Missing dimensions in met file: {missing_dims}"

    existing_vars = dataset.variables.keys()
    missing_vars = [var for var in expected_vars if var not in existing_vars]
    assert not missing_vars, f"Missing variables in footprint file: {missing_vars}"

    assert len(dataset.inlet_height.values) == len(dataset.inlet_pressure.values)

    # test metadata

    metadata = met_data.metadata

    assert metadata["site"] == site
    assert metadata["network"] == network
    assert metadata["met_source"] == clean_string(met_source)
