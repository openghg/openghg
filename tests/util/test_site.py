from openghg.util import sites_in_network
from pathlib import Path
import pytest
from helpers import get_info_datapath

@pytest.mark.parametrize(
    "network,expected_site",
    [
        ("NOAA", "MHD"),
        # ("AGAGE", "MHD"),
        # ("ICOS", "MHD"),
        # ("icos", "MHD"),
        # ("DECC", "TAC"),
        # ("nonetwork", None)
    ]
)
def test_sites_in_network(network, expected_site):
    """
    Test that network data can be extracted from a file in the correct format
    where format matches to 'openghg/data/site_info.json' file.
    """
    site_filepath = get_info_datapath("site_info.json")

    sites = sites_in_network(network=network, site_filepath=site_filepath)

    if expected_site is not None:
        assert expected_site in sites
    else:
        assert not sites
