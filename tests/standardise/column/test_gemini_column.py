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
    assert time[0] == Timestamp("2023-04-02T15:00:00")
    assert time[1] == Timestamp("2023-04-02T16:00:00")

    xch4 = data_ch4["xch4"].values
    assert np.isclose(xch4[0], 1888.025)
    assert np.isclose(xch4[-1], 1889.0175)

    expected_metadata = {
        "species": "ch4",
        "domain": domain,
        "inlet": "column",
        "site": "WEY",
        "network": "GEMINI",
        "platform": "site-column",
        "longitude": "-1.320",
        "latitude": "51.570",
        "data_owner": "Damien Weidmann",
        "data_owner_email": "<damien.weidmann@stfc.ac.uk>",
        "file_start_date": "2023-04-02",
        "file_end_date": "2023-04-02",
        "file_format_version": "2020.B",
        "data_revision": "R0",
        "description": "TCCON data standardised from hw20230402_20230402.public.qc.nc, with the pressure weights estimated via 'pressure_weight'.",
        "calibration_scale": "WMO CH4 X2004",
    }

    metadata = output_ch4["metadata"]
    assert metadata.items() >= expected_metadata.items()

    expected_attributes = expected_metadata
    expected_attributes.update({"longitude": "-1.320", "latitude": "51.570"})
    attributes = data_ch4.attrs
    assert attributes.items() >= expected_metadata.items()