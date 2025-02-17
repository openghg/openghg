import pytest
from helpers import get_footprint_datapath
from openghg.standardise.footprints import parse_acrg_org
from openghg.types import ParseError


def test_footprint_fail_message():
    """
    Test the parse_acrg_org function raises a ParseError and advises using "paris"
    source_format if 'srr' variable is detected.
    """
    fp_filepath = get_footprint_datapath("MHD-10magl_NAME_UKV_TEST_inert_PARIS-format_201301.nc")

    site = "mhd"
    inlet = "10m"
    domain = "test"
    model = "NAME"
    met_model = "ukv"
    species = "inert"

    with pytest.raises(ParseError) as exc:

        parse_acrg_org(
            filepath=fp_filepath,
            site=site,
            domain=domain,
            inlet=inlet,
            model=model,
            met_model=met_model,
            species=species,
        )

        assert "need to use source_format='paris'" in exc


def test_parse_acrg_org_site_key():
    """
    Tests the key created in the parser output for site data
    """

    datapath = get_footprint_datapath("WAO-20magl_UKV_rn_TEST_201801.nc")

    site = "WAO"
    inlet = "20m"
    model = "NAME"
    met_model = "UKV"
    species = "Rn"
    domain = "BRAZIL"

    result = parse_acrg_org(model=model,
                   inlet=inlet,
                   species=species,
                   filepath=datapath,
                   domain=domain,
                   site=site
                   )

    expected_key = f"{site}_{domain}_{model}_{inlet}"
    assert expected_key in result


def test_parse_acrg_org_satellite_key():
    """
    Tests the key created in the parser output for satellite data
    """

    datapath = get_footprint_datapath("GOSAT-BRAZIL-column_TEST_SA_201004_compressed.nc")

    satellite = "GOSAT"
    network = "GOSAT"
    domain = "TEST_SA"
    obs_region = "TEST_SA"
    model = "NAME"
    species = "ch4"
    inlet = "column"

    result = parse_acrg_org(model=model,
                   filepath=datapath,
                   satellite=satellite,
                   species=species,
                   domain=domain,
                   obs_region=obs_region,
                   inlet=inlet,
                   continuous=False
                   )

    expected_key = f"{satellite}_{obs_region}_{domain}_{model}_{inlet}"
    assert expected_key in result
