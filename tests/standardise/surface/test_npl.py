import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_npl

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def npl_data():
    filepath = get_surface_datapath(filename="NPL_test.csv", source_format="LGHG")
    data = parse_npl(data_filepath=filepath, sampling_period="60")
    return data


def test_read_file(npl_data):
    parsed_surface_metachecker(data=npl_data)

    co2_data = npl_data["co2"]["data"]
    ch4_data = npl_data["ch4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2020-06-12")
    assert co2_data["co2"][0] == pytest.approx(424.1672774)
    assert co2_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert co2_data["co2"][-1] == pytest.approx(419.9544809)

    assert ch4_data.time[0] == pd.Timestamp("2020-06-12")
    assert ch4_data["ch4"][0] == pytest.approx(2004.462127)
    assert ch4_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1910.546256)

    # TODO: Add metadata / attribute checks?


@pytest.mark.cfchecks
def test_npl_cf_compliance(npl_data):
    co2_data = npl_data["co2"]["data"]
    assert check_cf_compliance(dataset=co2_data)
