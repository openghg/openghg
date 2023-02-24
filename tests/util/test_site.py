from openghg.util import sites_in_network
from pathlib import Path
import pytest
from helpers import get_info_datapath


def test_site_info_file_mock():
    """
    Test site_info_file from openghg_defs is being successfully mocked.

    Because site_info_file is an external file (which can be updated), we have 
    added a mock for an internal version when site_info_file is imported
    which is static.

    This is mocked using an autouse fixture in tests/conftest.py/site_info_mock
    """
    from openghg_defs import site_info_file

    # Check local filepath is being used when external module is called.
    expected_location_end = Path("openghg/tests/data/info/site_info.json")

    assert site_info_file.parts[-5:] == expected_location_end.parts


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
    Test that network data can be extracted from a file in the correct format
    where format matches to 'openghg/data/site_info.json' file.
    """

    site_filename = get_info_datapath("site_info.json")

    sites = sites_in_network(network, site_filename=site_filename)

    if expected_site is not None:
        assert expected_site in sites
    else:
        assert not sites
