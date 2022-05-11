import os
import pytest
from openghg.util import site_code_finder, verify_site, read_header, to_lowercase
from openghg.types import InvalidSiteError


def test_read_header():
    filename = "header_test.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data"
    filepath = os.path.join(dir_path, test_data, filename)

    header = read_header(filepath=filepath)

    assert len(header) == 7


def test_verify_site():
    site = "BSD"
    result = verify_site(site=site)

    assert result == "bsd"

    site = "tac"
    result = verify_site(site=site)

    assert result == "tac"

    site = "tacolneston"
    result = verify_site(site=site)

    assert result == "tac"

    site = "atlantis"

    with pytest.raises(InvalidSiteError):
        result = verify_site(site=site)

    site = "cape"

    with pytest.raises(InvalidSiteError):
        result = verify_site(site=site)

    site = "india"

    with pytest.raises(InvalidSiteError):
        result = verify_site(site=site)


def test_to_lowercase():
    d = {"THIS": "ISANUPPERCASE", "spam": {"ALSO_BIG": "FOO", "BAAAR": 123}}

    lower = to_lowercase(d)

    assert lower == {"this": "isanuppercase", "spam": {"also_big": "foo", "baaar": 123}}


def test_site_code_finder():
    assert site_code_finder("heathfield") == "HFD"
    assert site_code_finder("monte_cimone") == "CMN"
    assert site_code_finder("cape verde") == "CVO"
    assert site_code_finder("jungfraujoch") == "JFJ"

    assert site_code_finder("nonsensical") is None
