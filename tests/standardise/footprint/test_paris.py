import pytest
from helpers import get_footprint_datapath
from openghg.standardise.footprint import parse_paris
from openghg.types import ParseError


@pytest.mark.parametrize(
    "site,inlet,model,met_model,filename",
    [
        (
            "mhd",
            "10m",
            "NAME",
            "ukv",
            "MHD-10magl_NAME_UKV_TEST_inert_PARIS-format_201301.nc",
        ),
        (
            "mhd",
            "10m",
            "FLEXPART",
            "ecmwfhres",
            "MHD-10magl_FLEXPART_ECMWFHRES_TEST_inert_201809.nc",
        ),
    ],
)
def test_paris_footprint(site,inlet,model,met_model,filename):
    """
    Test the parse_paris function is able to parse data in expected format.
    Note: Does not currently check data, just metadata.
    """
    fp_filepath = get_footprint_datapath(filename)

    domain = "test"
    species = "inert"

    data = parse_paris(filepath = fp_filepath,
                       site = site,
                       domain = domain,
                       inlet = inlet,
                       model = model,
                       met_model = met_model,
                       species = species,
    )

    fp_data = list(data.values())[0]

    metadata = fp_data["metadata"]

    expected_metadata = {
        "site": site,
        "inlet": inlet,
        "model": model,
        "domain": domain,
        "species": species,
        "data_type": "footprints",
    }

    assert metadata.items() >= expected_metadata.items()

    # TODO: Add data checks as required (may not be able to easily parameterize)


def test_paris_footprint_fail_message():
    """
    Test the parse_paris function raises a ParseError and advises using "acrg_org"
    source_format if 'fp' variable is detected.
    """
    fp_filepath = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    inlet = "10m"
    domain = "EUROPE"
    model = "test_model"
    species = "inert"

    with pytest.raises(ParseError) as exc:

        parse_paris(filepath = fp_filepath,
                    site = site,
                    domain = domain,
                    inlet = inlet,
                    model = model,
                    species=species,
        )

        assert "need to use source_format='acrg_org'" in exc