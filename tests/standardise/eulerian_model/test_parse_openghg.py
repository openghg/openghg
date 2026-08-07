import xarray as xr
from helpers import get_eulerian_datapath
from openghg.standardise.eulerian_model import parse_openghg


def test_parse_openghg():
    """This tests the parser for Eulerian model
    Looks for processed key and metadata associated with the file"""

    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = parse_openghg(filepath=test_datapath, model="GEOSChem", species="ch4")

    # TODO: Remove date from the key name for eulerian model
    assert "geoschem_ch4_2015-01-01" in proc_results

    metadata = proc_results["geoschem_ch4_2015-01-01"]["metadata"]
    assert "eulerian_model" in metadata["data_type"]
    assert "2015-01-01 00:00:00z" in metadata["simulation_start_date_and_time"]


def test_parse_openghg_data():
    """Test parsing Eulerian model data supplied directly."""
    filepath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    with xr.open_dataset(filepath) as dataset:
        results = parse_openghg(data=dataset.load(), model="GEOSChem", species="ch4")

    assert "geoschem_ch4_2015-01-01" in results
    assert results["geoschem_ch4_2015-01-01"]["metadata"]["data_type"] == "eulerian_model"


def test_parse_openghg_scalar_time_data_matches_filepath(tmp_path):
    """Direct and filepath inputs both expand a scalar time coordinate."""
    filepath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    with xr.open_dataset(filepath) as dataset:
        scalar_time_data = dataset.load().isel(time=0)

    scalar_filepath = tmp_path / "scalar_time.nc"
    scalar_time_data.to_netcdf(scalar_filepath)

    direct_result = parse_openghg(data=scalar_time_data, model="GEOSChem", species="ch4")
    filepath_result = parse_openghg(filepath=scalar_filepath, model="GEOSChem", species="ch4")

    direct_data = direct_result["geoschem_ch4_2015-01-01"]["data"]
    filepath_data = filepath_result["geoschem_ch4_2015-01-01"]["data"]
    assert direct_data.sizes["time"] == 1
    xr.testing.assert_equal(direct_data, filepath_data)


def test_parse_openghg_multiple():
    """This tests the parser for Eulerian model
    Looks for processed key and metadata associated with the file"""

    test_datapath1 = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")
    test_datapath2 = get_eulerian_datapath("GEOSChem.SpeciesConc.20150201_0000z_reduced.nc4")

    test_datapath = [test_datapath1, test_datapath2]

    proc_results = parse_openghg(filepath=test_datapath, model="GEOSChem", species="ch4")

    # TODO: Remove date from the key name for eulerian model
    assert "geoschem_ch4_2015-01-16" in proc_results

    metadata = proc_results["geoschem_ch4_2015-01-16"]["metadata"]
    assert metadata["start_date"] == "2015-01-16 12:00:00+00:00"
    assert metadata["end_date"] == "2015-02-16 12:00:00+00:00"
