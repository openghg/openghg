import pytest
from pathlib import Path

import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

from HUGS.Modules import Datasource, ThamesBarrier
from HUGS.ObjectStore import get_local_bucket


def test_site_attributes():
    tb = ThamesBarrier()

    site_attributes = tb.site_attributes()

    assert site_attributes["data_owner"] == "Valerio Ferracci"
    assert site_attributes["data_owner_email"] == "V.Ferracci@cranfield.ac.uk"
    assert site_attributes["Notes"] == "~5m above high tide water level, in tidal region of the Thames"
    assert site_attributes["inlet_height_magl"] == "5 m"
    assert site_attributes["instrument"] == "Picarro G2401"

def test_read_file():
    _ = get_local_bucket(empty=True)

    filename = "7_Jul_2019_calibrated_data.csv"
    data_path = "/home/gar/Documents/Devel/hugs/raw_data/thames_data"

    filepath = Path(data_path).joinpath(filename)

    tb = ThamesBarrier()

    uuids = tb.read_file(data_filepath=filepath, source_name="TMB")

    ch4_ds = Datasource.load(uuid=uuids["TMB_CH4"])
    co2_ds = Datasource.load(uuid=uuids["TMB_CO2"])
    co_ds = Datasource.load(uuid=uuids["TMB_CO"])

    print(ch4_ds._data)



    

