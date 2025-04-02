import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_niwa

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_niwa_parser():
    "To test the NIWA network file parser"
    filepath = get_surface_datapath(filename="niwa.nc", source_format="NIWA")

    data = parse_niwa(filepath=filepath, sampling_period="1h", site="LAU", inlet=10)

    assert "ch4" in data
