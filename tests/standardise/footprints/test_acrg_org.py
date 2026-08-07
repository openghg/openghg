import pytest
import xarray as xr
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
    # TODO: Remove test after keys declaration is removed from the parsers

    datapath = get_footprint_datapath("WAO-20magl_UKV_rn_TEST_201801.nc")

    site = "WAO"
    inlet = "20m"
    model = "NAME"
    met_model = "UKV"
    species = "Rn"
    domain = "BRAZIL"

    result = parse_acrg_org(
        model=model, inlet=inlet, species=species, filepath=datapath, domain=domain, site=site
    )

    expected_key = f"{site}_{domain}_{model}_{inlet}"
    assert expected_key in result


def test_parse_acrg_org_data():
    """Test parsing ACRG footprint data supplied directly."""
    filepath = get_footprint_datapath("WAO-20magl_UKV_rn_TEST_201801.nc")

    with xr.open_dataset(filepath) as dataset:
        result = parse_acrg_org(
            data=dataset.load(),
            model="NAME",
            inlet="20m",
            species="Rn",
            domain="BRAZIL",
            site="WAO",
        )

    assert "WAO_BRAZIL_NAME_20m" in result
    assert result["WAO_BRAZIL_NAME_20m"]["metadata"]["data_type"] == "footprints"


def test_parse_acrg_org_integrated_co2_drops_time_resolved_variables():
    """Explicit integrated CO2 parsing retains fp without HiTRes variables or dimensions."""
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    result = parse_acrg_org(
        filepath=datapath,
        site="TAC",
        inlet="100m",
        model="NAME",
        met_model="UKV",
        species="co2",
        domain="TEST",
        time_resolved=False,
    )

    data = result["TAC_TEST_NAME_100m"]["data"]
    assert "fp" in data
    assert "fp_HiTRes" not in data
    assert "H_back" not in data.dims


def test_parse_acrg_org_satellite_key():
    """
    Tests the key created in the parser output for satellite data
    """
    # TODO: Remove test after keys declaration is removed from the parsers
    datapath = get_footprint_datapath("GOSAT-BRAZIL-column_SOUTHAMERICA_201004_compressed.nc")

    satellite = "GOSAT"
    domain = "SOUTHAMERICA"
    obs_region = "BRAZIL"
    model = "NAME"
    species = "ch4"
    inlet = "column"

    result = parse_acrg_org(
        model=model,
        filepath=datapath,
        satellite=satellite,
        species=species,
        domain=domain,
        obs_region=obs_region,
        inlet=inlet,
        continuous=False,
        period="varies",
    )

    expected_key = f"{satellite}_{obs_region}_{domain}_{model}_{inlet}"
    assert expected_key in result
