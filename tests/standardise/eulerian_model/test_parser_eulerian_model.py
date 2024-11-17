import pytest
from helpers import get_eulerian_datapath, clear_test_store
from openghg.retrieve import search
from openghg.standardise import standardise_eulerian
from xarray import open_dataset
from openghg.standardise.eulerian_model import parse_eulerian_model


def test_read_file():
    """ This tests the parser for Eulerian model
    Looks for processed key and metadata associated with the file"""

    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = parse_eulerian_model(filepath=test_datapath, model="GEOSChem", species="ch4")

    assert "geoschem_ch4_2015-01-01" in proc_results

    assert "eulerian_model" in proc_results["geoschem_ch4_2015-01-01"]["metadata"]["data_type"]
    assert "2015-01-01 00:00:00+00:00" in proc_results["geoschem_ch4_2015-01-01"]["metadata"]["start_date"]
    assert "2016-01-01 00:00:00+00:00" in proc_results["geoschem_ch4_2015-01-01"]["metadata"]["end_date"]
    assert "ch4" in proc_results["geoschem_ch4_2015-01-01"]["metadata"]["species"]


