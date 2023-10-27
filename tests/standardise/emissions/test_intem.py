import logging
from helpers import get_emissions_datapath
from openghg.standardise.emissions import parse_intem
from pandas import Timestamp
from openghg.standardise.meta import metadata_default_keys

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file():
    """
    Test file in OpenGHG format (variables and attributes) can be
    correctly parsed.
    """
    filepath = get_emissions_datapath(filename="ch4_intem.nc")
    domain = "europe"
    species = "ch4"

    data = parse_intem(filepath,
                         domain=domain,
                         species=species)
    assert "ch4_intem_europe" in data
    output_ch4 = data["ch4_intem_europe"]
    data_ch4 = output_ch4["data"]
    
    time = data_ch4["time"]
    assert time[0] == Timestamp("2012-01-01T00:00:00.000000000")
    assert time[-1] == Timestamp("2012-10-01T00:00:00.000000000")

    attributes = data_ch4.attrs

    metadata_keys = metadata_default_keys()
    expected_metadata = {param: value for param, value in attributes.items() if param in metadata_keys}

    metadata = output_ch4["metadata"]
    assert metadata.items() >= expected_metadata.items()

