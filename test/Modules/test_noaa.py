import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

import os
import pandas as pd
import pytest

from HUGS.Modules import Datasource, NOAA
from HUGS.ObjectStore import get_local_bucket

def test_read_file():
    noaa = NOAA()

    _ = get_local_bucket(empty=True)

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)

    uuids = noaa.read_file(data_filepath=filepath, species="CO")

    co_ds = Datasource.load(uuid=uuids["co_pocn25_surface_CO"])

    print(co_ds._data["2017-03-25-02:00:00+00:00_2017-05-20-07:05:00+00:00"].time)

def test_read_data():
    noaa = NOAA()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)

    data = noaa.read_data(data_filepath=filepath, species="CO")

    co_data = data["CO"]["data"]
    metadata  = data["CO"]["metadata"]
    attributes = data["CO"]["attributes"]

    correct_metadata = {'species': 'CO', 'site': 'POC', 'measurement_type': 'flask'}

    correct_attributes = {'data_owner': 'Ed Dlugokencky, Gabrielle Petron (CO)', 
                        'data_owner_email': 'ed.dlugokencky@noaa.gov, gabrielle.petron@noaa.gov', 
                        'inlet_height_magl': 'NA', 
                        'instrument': {'CH4': 'GC-FID', 'C2H6': 'GC-FID', 'CO2': 'NDIR', 'CH4C13': 'IRMS', 'CO': 'GC-HgO-VUV'}}

    
    assert co_data["CO"][0] == 94.9
    assert co_data["CO"][-1] == 73.16
    assert co_data.time[0] == pd.Timestamp("1990-06-29T05:00:00.000000000")
    assert co_data.time[-1] == pd.Timestamp("2017-07-15T04:15:00.000000000")

    assert metadata == correct_metadata
    assert attributes == correct_attributes
    
