import logging
import pandas as pd
import pytest

from openghg.standardise.surface import parse_npl
from helpers import get_datapath, parsed_surface_metachecker

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file():

    filepath = get_datapath(filename="NPL_test.csv", data_type="LGHG")

    data = parse_npl(data_filepath=filepath, sampling_period=60)

    parsed_surface_metachecker(data=data)

    co2_data = data["CO2"]["data"]
    ch4_data = data["CH4"]["data"]

    assert co2_data.time[0] == pd.Timestamp("2020-06-12")
    assert co2_data["co2"][0] == pytest.approx(424.1672774)
    assert co2_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert co2_data["co2"][-1] == pytest.approx(419.9544809)

    assert ch4_data.time[0] == pd.Timestamp("2020-06-12")
    assert ch4_data["ch4"][0] == pytest.approx(2004.462127)
    assert ch4_data.time[-1] == pd.Timestamp("2020-07-01T00:24:00")
    assert ch4_data["ch4"][-1] == pytest.approx(1910.546256)
