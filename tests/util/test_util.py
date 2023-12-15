import os

import pytest
from openghg.types import InvalidSiteError
from openghg.util import (
    read_header,
    read_local_config,
    running_in_cloud,
    running_locally,
    running_on_hub,
    site_code_finder,
    to_lowercase,
    verify_site,
)


def test_running_locally(monkeypatch):
    monkeypatch.setenv("OPENGHG_PATH", "/tmp/this_that")
    assert running_locally()

    monkeypatch.setenv("OPENGHG_CLOUD", "1")
    monkeypatch.setenv("OPENGHG_HUB", "1")

    assert running_in_cloud()
    assert running_on_hub()

    assert not running_locally()

    monkeypatch.setenv("OPENGHG_CLOUD", "0")

    assert not running_in_cloud()
    assert not running_locally()

    monkeypatch.setenv("OPENGHG_HUB", "0")

    assert not running_on_hub()

    assert running_locally()


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

    site = "cape"
    result = verify_site(site=site)
    assert result is None

    site = "mars"
    result = verify_site(site=site)
    assert result is None


def test_to_lowercase():
    d = {"THIS": "ISANUPPERCASE", "spam": {"ALSO_BIG": "FOO", "BAAAR": 123}}

    lower = to_lowercase(d)

    assert lower == {"this": "isanuppercase", "spam": {"also_big": "foo", "baaar": 123}}

    skip_keys = ["THIS"]

    with_skipped = to_lowercase(d, skip_keys=skip_keys)

    assert with_skipped == {"spam": {"also_big": "foo", "baaar": 123}, "THIS": "ISANUPPERCASE"}


def test_site_code_finder():
    assert site_code_finder("heathfield") == "hfd"
    assert site_code_finder("monte_cimone") == "cmn"
    assert site_code_finder("shangdianzi") == "sdz"
    assert site_code_finder("jungfraujoch") == "jfj"

    assert site_code_finder("nonsensical") is None
