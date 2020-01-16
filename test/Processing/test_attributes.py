from pathlib import Path
import pytest
import os
import tempfile

# from cfchecker import CFChecker

from HUGS.Processing import get_attributes
from HUGS.Modules import CRDS, EUROCOM
from HUGS.ObjectStore import get_local_bucket


import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def test_eurocom_attributes():
    _  = get_local_bucket(empty=True)

    euro = EUROCOM.load()
    # dir_path = os.path.dirname(__file__)
    # test_data = "/Users/wm19361/Documents/Devel/hugs/raw_data/eurocom/"
    filename = "tac.picarro.1minute.100m.test.dat"

    filepath = "/Users/wm19361/Documents/Devel/hugs/raw_data/eurocom/MHD_air.hdf.all.COMBI_Drought2018_20190522.co2"

    data = euro.read_data(data_filepath=filepath, site="MHD")

    assert False


def test_crds_attributes():
    _ = get_local_bucket(empty=True)

    crds = CRDS.load()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "tac.picarro.1minute.100m.test.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    filepath = Path(filepath)

    combined = crds.read_data(data_filepath=filepath, site="tac")

    combined_attributes = crds.assign_attributes(data=combined, site="tac")

    # for key in combined_attributes:
    #     ds = combined_attributes[key]["data"]
    #     ds.to_netcdf(f"/tmp/testfile_{key}.nc")

    ch4_data = combined_attributes["ch4"]["data"]
    co2_data = combined_attributes["co2"]["data"]

    ch4_attr = ch4_data.attrs
    co2_attr = co2_data.attrs

    ch4_attr_complete = ch4_attr.copy()
    co2_attr_complete = co2_attr.copy()

    del ch4_attr["File created"]
    del co2_attr["File created"]
    del ch4_attr["species"]
    del co2_attr["species"]
    del ch4_attr["Calibration_scale"]
    del co2_attr["Calibration_scale"]

    global_attributes = {'data_owner': "Simon O'Doherty", 'data_owner_email': 's.odoherty@bristol.ac.uk', 'inlet_height_magl': 'test', 
                        'comment': 'Cavity ring-down measurements. Output from GCWerks', 
                        'Conditions of use': 'Ensure that you contact the data owner at the outset of your project.', 
                        'Source': 'In situ measurements of air', 'Conventions': 'CF-1.6', 'Processed by': 'auto@hugs-cloud.com', 
                        'station_longitude': 1.13872, 'station_latitude': 52.51775, 
                        'station_long_name': 'Tacolneston Tower, UK', 'station_height_masl': 50.0}

    assert ch4_attr == global_attributes
    assert co2_attr == global_attributes

    assert ch4_attr_complete["species"] == "ch4"
    assert co2_attr_complete["species"] == "co2"

    assert ch4_attr_complete["Calibration_scale"] == "NOAA-2004A"
    assert co2_attr_complete["Calibration_scale"] == "NOAA-2007"

    # Check the individual variables attributes
    
    time_attributes = {'label': 'left', 'standard_name': 'time',
                       'comment': 'Time stamp corresponds to beginning of sampling period. Time since midnight UTC of reference date. Note that sampling periods are approximate.', 
                       'sampling_period_seconds': 60}

    assert ch4_data.time.attrs == time_attributes
    assert co2_data.time.attrs == time_attributes

    # Check individual variables
    assert ch4_data["ch4_count"].attrs == {'long_name': 'mole_fraction_of_methane_in_air_count', 'units': '1e-9'}
    assert ch4_data["ch4_stdev"].attrs == {'long_name': 'mole_fraction_of_methane_in_air_stdev', 'units': '1e-9'}
    assert ch4_data["ch4_n_meas"].attrs == {'long_name': 'mole_fraction_of_methane_in_air_n_meas'}

    assert co2_data["co2_count"].attrs == {'long_name': 'mole_fraction_of_carbon_dioxide_in_air_count', 'units': '1e-6'}
    assert co2_data["co2_stdev"].attrs == {'long_name': 'mole_fraction_of_carbon_dioxide_in_air_stdev', 'units': '1e-6'}
    assert co2_data["co2_n_meas"].attrs == {'long_name': 'mole_fraction_of_carbon_dioxide_in_air_n_meas'}

    # with tempfile.TemporaryDirectory() as tmpdir:
    #     files = []
    #     # Write files to tmpdir and call cfchecker
    #     for key in combined_attributes:
    #         dataset = combined_attributes[key]["data"]
    #         filepath = Path(tmpdir).joinpath(f"test_{key}.nc")
    #         dataset.to_netcdf(filepath)
    #         files.append(filepath)

    #     checker = CFChecker(version="1.6", silent=True)

    #     for f in files:
    #         checker.checker(str(f))

    #         results = checker.get_total_counts()

    #         assert results["FATAL"] == 0
    #         assert results["ERROR"] == 0
    #         assert results["WARN"] < 3
            
        
        
