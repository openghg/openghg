import os
from openghg import util


def test_read_header():
    filename = "header_test.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data"
    filepath = os.path.join(dir_path, test_data, filename)

    header = util.read_header(filepath=filepath)

    assert len(header) == 7


def test_valid_site():
    site = "BSD"
    result = util.valid_site(site=site)

    assert result is True

    site = "tac"
    result = util.valid_site(site=site)

    assert result is True

    site = "Dover"
    result = util.valid_site(site=site)

    assert result is False


def test_to_lowercase():
    d = {"THIS": "ISANUPPERCASE", "spam": {"ALSO_BIG": "FOO", "BAAAR": 123}}

    lower = util.to_lowercase(d)

    assert lower == {'this': 'isanuppercase', 'spam': {'also_big': 'foo', 'baaar': 123}}
