from helpers import get_eulerian_datapath
from openghg.standardise.eulerian_model import parse_openghg


def test_read_file():
    """ This tests the parser for Eulerian model
    Looks for processed key and metadata associated with the file"""

    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = parse_openghg(filepath=test_datapath, model="GEOSChem", species="ch4")
