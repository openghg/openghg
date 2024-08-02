import pytest
from helpers import get_footprint_datapath
from openghg.standardise.footprint import parse_paris

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
