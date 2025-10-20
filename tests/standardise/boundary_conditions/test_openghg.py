import logging

from helpers import get_bc_datapath
from openghg.standardise.boundary_conditions import parse_openghg

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_parse_openghg():
    """
    To test the parser for boundary condititons
    """

    data_path = get_bc_datapath(filename="ch4_EUROPE_201208.nc")

    results = parse_openghg(filepath=data_path, species="ch4", bc_input="MOZART", domain="EUROPE")

    metadata = results["ch4_mozart_europe"]["metadata"]

    assert "mozart" in metadata["bc_input"]
    assert "europe" in metadata["domain"]
    assert "ch4" in metadata["species"]


def test_parse_openghg_multi_file_1():
    """
    Test the parser for boundary conditions is able to accept multiple files using known domain.
    """

    data_path_1 = get_bc_datapath(filename="ch4_EUROPE_201208.nc")
    data_path_2 = get_bc_datapath(filename="ch4_EUROPE_201209.nc")

    data_path = [data_path_1, data_path_2]

    results = parse_openghg(filepath=data_path, species="ch4", bc_input="MOZART", domain="EUROPE")

    metadata = results["ch4_mozart_europe"]["metadata"]

    assert "mozart" in metadata["bc_input"]
    assert "europe" in metadata["domain"]
    assert "ch4" in metadata["species"]


def test_parse_openghg_multi_file_2():
    """
    Test the parser for boundary conditions is able to accept multiple files when files contain 0D 'time' dimension.
    This is from a slightly incorrectly formatted file but is reasonable to try and help people correct this rather than asking for a new file.
    """

    data_path_1 = get_bc_datapath(filename="co2_TEST_201407.nc")
    data_path_2 = get_bc_datapath(filename="co2_TEST_201408.nc")

    data_path = [data_path_1, data_path_2]

    results = parse_openghg(filepath=data_path, species="co2", bc_input="CAMS", domain="TEST")

    metadata = results["co2_cams_test"]["metadata"]

    assert "cams" in metadata["bc_input"]
    assert "test" in metadata["domain"]
    assert "co2" in metadata["species"]
