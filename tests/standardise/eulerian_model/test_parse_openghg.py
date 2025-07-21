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
