import os
from pathlib import Path

import pytest
from openghg.types import InvalidSiteError
from openghg.util import (
    read_header,
    read_local_config,
    site_code_finder,
    synonyms,
    to_lowercase,
    verify_site,
    sort_by_filenames,
)

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


def test_synonyms():
    """Test to check is species value is passed as Inert it should return the same"""

    species = synonyms("Inert")

    assert species == "inert"

    with pytest.raises(ValueError):
        synonyms(species="openghg", allow_new_species=False)


def test_file_sorting():
    """
    Testing sorting of filenames
    """

    filepaths = [
        "DECC-picarro_TAC_20130131_co2-185m-20220929.nc",
        "DECC-picarro_TAC_20130131_co2-185m-20220928.nc",
    ]

    sorted_filepaths = sort_by_filenames(filepaths)

    assert sorted_filepaths[1] == Path(filepaths[0])


def test_sorting_with_str():
    """
    Testing if only string value is passed
    """

    filepaths = "DECC-picarro_TAC_20130131_co2-185m-20220929.nc"

    sorted_file = sort_by_filenames(filepaths)

    assert isinstance(sorted_file, list)
