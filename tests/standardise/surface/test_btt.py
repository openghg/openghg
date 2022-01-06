import logging
import pandas as pd
import pytest

from openghg.standardise.surface import parse_btt
from helpers import get_datapath, combined_surface_metachecker

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file():
    filepath = get_datapath(filename="BTT_test.csv", data_type="LGHG")

    data = parse_btt(data_filepath=filepath)

    combined_surface_metachecker(data=data)

    co2_data = data["CO2"]["data"]
    ch4_data = data["CH4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert co2_data["co2"][0] == pytest.approx(420.4700671)
    assert co2_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert co2_data["co2"][-1] == pytest.approx(413.45942912)

    assert ch4_data.time[0] == pd.Timestamp("2019-01-14T09:30:00.00")
    assert ch4_data["ch4"][0] == pytest.approx(1957.23980459)
    assert ch4_data.time[-1] == pd.Timestamp("2019-01-14T14:00:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1961.72216725)


