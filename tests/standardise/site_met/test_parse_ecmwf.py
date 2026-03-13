import pytest

from openghg.util import clean_string
from openghg.standardise.site_met import parse_ecmwf
from helpers import get_met_datapath


@pytest.mark.parametrize(
    "site,network,filename",
    [
        (
            "mhd",
            "AGAGE",
            "Met_mhd_agage_201608.nc",
        ),
        (
            "tac",
            "AGAGE",
            "Met_tac_agage_201608.nc",
        ),
    ],
)
def test_ecmwf_site_met(site, network, filename):

    met_filepath = get_met_datapath(filename)

    data = parse_ecmwf(met_filepath, site, network)
    dataset = data[0].data

    # test dataset

    assert dataset is not None

    expected_dims = ["time", "lat", "lon", "pressure_level", "inlet"]

    expected_vars = ["u_wind", "v_wind"]

    existing_dims = dataset.variables.keys()
    missing_dims = [var for var in expected_dims if var not in existing_dims]
    assert not missing_dims, f"Missing dimensions in met file: {missing_dims}"

    existing_vars = dataset.variables.keys()
    missing_vars = [var for var in expected_vars if var not in existing_vars]
    assert not missing_vars, f"Missing variables in footprint file: {missing_vars}"

    assert len(dataset.inlet.values) == len(dataset.inlet_pressure.values)

    # test metadata

    metadata = data[0].metadata

    assert metadata["site"] == site
    assert metadata["network"] == network
    assert metadata["met_source"] == clean_string("ECMWF ERA5")
