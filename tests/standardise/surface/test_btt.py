import logging
import pandas as pd
import pytest

from openghg.standardise.surface import parse_btt
from helpers import get_datapath, parsed_surface_metachecker, check_cf_compliance

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def btt_data():
    filepath = get_datapath(filename="BTT_test.csv", data_type="LGHG")
    data = parse_btt(data_filepath=filepath)
    return data


@pytest.mark.xfail(reason="Bug: No inlet or instrument keys in metadata, check if required - see #201")
def test_read_file(btt_data):
    parsed_surface_metachecker(data=btt_data)

    co2_data = btt_data["CO2"]["data"]
    ch4_data = btt_data["CH4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert co2_data["co2"][0] == pytest.approx(420.4700671)
    assert co2_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert co2_data["co2"][-1] == pytest.approx(413.45942912)

    assert ch4_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert ch4_data["ch4"][0] == pytest.approx(1957.23980459)
    assert ch4_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1961.72216725)


@pytest.mark.cfchecks
def test_btt_cf_compliance(btt_data):
    co2_data = btt_data["CO2"]["data"]
    assert check_cf_compliance(dataset=co2_data)
