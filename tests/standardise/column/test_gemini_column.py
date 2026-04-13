import logging

import numpy as np
import pytest
from helpers import get_column_datapath  # , parsed_surface_metachecker, check_cf_compliance
from openghg.standardise.column import parse_gemini
from openghg.standardise.meta import attributes_default_keys
from pandas import Timestamp

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def test_parse_gemini():
    """
    Test file in TCCON format (variables and attributes) can be
    correctly parsed.
    """
    filepath = get_column_datapath(filename="gemini_uk_SN196_wey_241122.nc")

    domain = "EUROPE"
    species = "ch4"
    pressure_weights_method = "pressure_weight"

    data = parse_gemini(
        filepath,
        site="wey",
        pressure_weights_method=pressure_weights_method,
        domain=domain,
        species=species,
    )

    assert "ch4" in data

    assert "integration_operator" not in data
    assert "ak_altitude" not in data
    assert "prior_altitude" not in data

    output_ch4 = data["ch4"]
    data_ch4 = output_ch4["data"]

    time = data_ch4["time"]
    assert time[0] == Timestamp("2024-11-22T10:00:00")
    assert time[1] == Timestamp("2024-11-22T11:00:00")

    xch4 = data_ch4["xch4"].values
    assert np.isclose(xch4[0], 1908.16)
    assert np.isclose(xch4[-1], 1908.43)

    expected_metadata = {
        "species": "ch4",
        "domain": domain,
        "inlet": "column",
        "site": "WEY",
        "network": "GEMINI",
        "platform": "site-column",
        "longitude": "1.123",
        "latitude": "52.951",
        "data_owner": "Neil Humpage",
        "data_owner_email": "nh58@leicester.ac.uk",
    }

    metadata = output_ch4["metadata"]
    assert metadata.items() >= expected_metadata.items()
