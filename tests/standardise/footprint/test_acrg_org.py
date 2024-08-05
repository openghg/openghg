import pytest
from helpers import get_footprint_datapath
from openghg.standardise.footprint import parse_acrg_org
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
    met_model="ukv"
    species = "inert"

    with pytest.raises(ParseError) as exc:

        parse_acrg_org(filepath = fp_filepath,
                    site = site,
                    domain = domain,
                    inlet = inlet,
                    model = model,
                    met_model = met_model,
                    species=species,
        )

        assert "need to use source_format='paris'" in exc
