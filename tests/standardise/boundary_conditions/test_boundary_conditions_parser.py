import logging

from helpers import get_bc_datapath
from openghg.standardise.boundary_conditions import parse_boundary_conditions
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_parse_boundary_conditions():
    """
    To test the parser for boundary condititons
    """

    data_path = get_bc_datapath(filename="ch4_EUROPE_201208.nc")

    results = parse_boundary_conditions(filepath=data_path,
                                        species="ch4",
                                        bc_input="MOZART",
                                        domain="EUROPE",
                                        )

    metadata = results["ch4_mozart_europe"]["metadata"]

    assert "mozart" in metadata["bc_input"]
    assert "europe" in metadata["domain"]
    assert "ch4" in metadata["species"]
