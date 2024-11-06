import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_co2_games

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def test_parse_co2_games():
    co2_games_data = get_surface_datapath(filename="co2_bsd_tower-insitu_160_allvalid-108magl.nc",
                                          source_format="co2_games")
    data = parse_co2_games(filepath=co2_games_data,
                           site="BSD",
                           measurement_type="insitu")