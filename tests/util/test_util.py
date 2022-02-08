import os
import pytest
from openghg import util
from openghg.types import InvalidSiteError


def test_read_header():
    filename = "header_test.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data"
    filepath = os.path.join(dir_path, test_data, filename)

    header = util.read_header(filepath=filepath)

    assert len(header) == 7


def test_verify_site():
    site = "BSD"
    result = util.verify_site(site=site)

    assert result == "bsd"

    site = "tac"
    result = util.verify_site(site=site)

    assert result == "tac"

    site = "tacolneston"
    result = util.verify_site(site=site)

    assert result == "tac"

    site = "atlantis"

    with pytest.raises(InvalidSiteError):
        result = util.verify_site(site=site)

    site = "cape"

    with pytest.raises(InvalidSiteError):
        result = util.verify_site(site=site)

    site = "india"

    with pytest.raises(InvalidSiteError):
        result = util.verify_site(site=site)


def test_to_lowercase():
    d = {"THIS": "ISANUPPERCASE", "spam": {"ALSO_BIG": "FOO", "BAAAR": 123}}

    lower = util.to_lowercase(d)

    assert lower == {"this": "isanuppercase", "spam": {"also_big": "foo", "baaar": 123}}
