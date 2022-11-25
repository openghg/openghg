from openghg.util import sites_in_network
from pathlib import Path
import pytest
from helpers import get_info_datapath

@pytest.mark.parametrize(
    "network,expected_site",
    [
        ("NOAA", "MHD"),
        ("AGAGE", "MHD"),
        ("ICOS", "MHD"),
        ("icos", "MHD"),
        ("DECC", "TAC"),
        ("nonetwork", None)
    ]
)
def test_sites_in_network(network, expected_site):
    """
    Test that network data can be extracted from the acrg_site_info.json file.

    **Note**: This is dependent on external data which could change.
    Could update this to use a dummy file instead.
    """

    site_info_json = get_info_datapath("site_info.json")

    sites = sites_in_network(network, site_json=site_info_json)

    if expected_site is not None:
        assert expected_site in sites
    else:
        assert not sites
