import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_co2_games

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def test_parse_co2_games():

    data = parse_co2_games(filepath="/group/chem/acrg/ES/sharing/CO2/wur_paris_co2_sim/co2_bsd_tower-insitu_160_allvalid-108magl.nc",
                            site="BSD",
                           measurement_type="insitu")