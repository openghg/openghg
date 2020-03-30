import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

import os
import pandas as pd
import pytest

from HUGS.Modules import Datasource, NOAA
from HUGS.ObjectStore import get_local_bucket

def test_to_data():
    _ = get_local_bucket(empty=True)
    
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)
    uuids = NOAA.read_file(data_filepath=filepath, species="CO")

    noaa = NOAA.load()

    data = noaa.to_data()

    assert data["stored"] == False
    assert list(data["datasource_uuids"].values()) == ["co_pocn25_surface_CO"]
    assert list(data["datasource_names"].keys()) == ["co_pocn25_surface_CO"]
    assert data["file_hashes"] == {'48ba8d093008359d836b3928bf4c9793fa564fee': 'co_pocn25_surface-flask_1_ccgg_event.txt'}

def test_from_data():
    _ = get_local_bucket(empty=True)

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)
    uuids = NOAA.read_file(data_filepath=filepath, species="CO")

    noaa = NOAA.load()

    data = noaa.to_data()

    noaa_2 = NOAA.from_data(data)

    assert noaa_2._stored == False
    assert list(noaa_2._datasource_uuids.values()) == ["co_pocn25_surface_CO"]
    assert list(noaa_2._datasource_names.keys()) == ["co_pocn25_surface_CO"]
    assert noaa_2._file_hashes == {'48ba8d093008359d836b3928bf4c9793fa564fee': 'co_pocn25_surface-flask_1_ccgg_event.txt'}


def test_read_file():
    _ = get_local_bucket(empty=True)

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)

    uuids = NOAA.read_file(data_filepath=filepath, species="CO")

    noaa = NOAA.load()

    co_ds = Datasource.load(uuid=uuids["co_pocn25_surface_CO"])

    date_key = "2017-03-25-02:00:00+00:00_2017-05-20-07:05:00+00:00"

    netcdf_attrs = {'Conditions of use': 'Ensure that you contact the data owner at the outset of your project.', 
    'Source': 'In situ measurements of air', 'Conventions': 'CF-1.6',
    'Processed by': 'auto@hugs-cloud.com', 'species': 'co', 'Calibration_scale': 'WMO CO_X2014A', 
    'station_longitude': 0.036995, 'station_latitude': 51.496769, 'station_long_name': 'Thames Barrier, London, UK', 
    'station_height_masl': 5.0}

    # Remove the file created timestamp
    file_attrs = co_ds._data[date_key].attrs
    del file_attrs["File created"]

    assert co_ds._data[date_key]["co"][0] == 117.22
    assert co_ds._data[date_key].time[0] == pd.Timestamp("2017-03-25T02:00:00.000000000")
    assert file_attrs == netcdf_attrs

    datasources = noaa.datasource_names()

    assert "co_pocn25_surface_CO" in datasources
    assert noaa._file_hashes["48ba8d093008359d836b3928bf4c9793fa564fee"] == "co_pocn25_surface-flask_1_ccgg_event.txt"


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
    
def test_upload_same_file():
    _ = get_local_bucket(empty=True)

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/NOAA"
    filename = "co_pocn25_surface-flask_1_ccgg_event.txt"

    filepath = os.path.join(dir_path, test_data, filename)

    uuids = NOAA.read_file(data_filepath=filepath, species="CO")

    with pytest.raises(ValueError):
        uuids = NOAA.read_file(data_filepath=filepath, species="CO")


