import logging
import pandas as pd
import pytest

from openghg.standardise.surface import parse_tmb
from helpers import get_datapath, parsed_surface_metachecker

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_read_file():
    filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    data = parse_tmb(data_filepath=filepath)

    parsed_surface_metachecker(data=data)

    ch4_data = data["CH4"]["data"]
    co2_data = data["CO2"]["data"]
    co_data = data["CO"]["data"]

    assert ch4_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert ch4_data["ch4"][0] == pytest.approx(1960.835716)
    assert ch4_data["ch4_variability"][0] == 0

    assert co2_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co2_data["co2"][0] == pytest.approx(417.973447)
    assert co2_data["co2_variability"][0] == 0

    assert co_data.time[0] == pd.Timestamp("2019-07-01T00:39:55.000000000")
    assert co_data["co"][0] == pytest.approx(0.08788712)
    assert co_data["co_variability"][0] == 0
