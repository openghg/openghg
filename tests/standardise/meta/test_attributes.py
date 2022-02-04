import logging
import os
from pathlib import Path

from openghg.standardise.surface import parse_crds
from openghg.objectstore import get_local_bucket
from openghg.standardise.meta import assign_attributes
from helpers import get_datapath, metadata_checker_obssurface, attributes_checker_obssurface

# import tempfile
# from cfchecker import CFChecker

# flake8: noqa

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_crds_attributes():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="tac.picarro.1minute.100m.test.dat", data_type="CRDS")

    combined = parse_crds(data_filepath=filepath, site="tac", network="DECC")

    combined_attributes = assign_attributes(data=combined, site="tac")

    ch4_data = combined_attributes["ch4"]["data"]
    co2_data = combined_attributes["co2"]["data"]

    ch4_attr = ch4_data.attrs
    co2_attr = co2_data.attrs

    del ch4_attr["file_created"]
    del co2_attr["file_created"]

    expected_ch4_attr = {
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "100m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "long_name": "tacolneston",
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "100m",
        "port": "9",
        "type": "air",
        "network": "DECC",
        "species": "ch4",
        "calibration_scale": "WMO-X2004A",
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "source": "In situ measurements of air",
        "Conventions": "CF-1.8",
        "processed_by": "OpenGHG_Cloud",
        "sampling_period_unit": "s",
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "Tacolneston Tower, UK",
        "station_height_masl": 50.0,
    }

    assert ch4_attr == expected_ch4_attr

    ch4_metadata = combined_attributes["ch4"]["metadata"]

    expected_ch4_metadata = {
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "100m",
        "port": "9",
        "type": "air",
        "network": "DECC",
        "species": "ch4",
        "calibration_scale": "WMO-X2004A",
        "long_name": "tacolneston",
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "Tacolneston Tower, UK",
        "station_height_masl": 50.0,
        "inlet_height_magl": "100m",
    }

    assert ch4_metadata == expected_ch4_metadata

    # Check the individual variables attributes
    time_attributes = {
        "label": "left",
        "standard_name": "time",
        "comment": "Time stamp corresponds to beginning of sampling period. Time since midnight UTC of reference date. Note that sampling periods are approximate.",
        "sampling_period_seconds": "60",
    }

    assert ch4_data.time.attrs == time_attributes
    assert co2_data.time.attrs == time_attributes

    # Check individual variables
    assert ch4_data["ch4"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air",
        "units": "1e-9",
    }
    assert ch4_data["ch4_variability"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air_variability",
        "units": "1e-9",
    }
    assert ch4_data["ch4_number_of_observations"].attrs == {
        "long_name": "mole_fraction_of_methane_in_air_number_of_observations"
    }

    assert co2_data["co2"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air",
        "units": "1e-6",
    }
    assert co2_data["co2_variability"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air_variability",
        "units": "1e-6",
    }
    assert co2_data["co2_number_of_observations"].attrs == {
        "long_name": "mole_fraction_of_carbon_dioxide_in_air_number_of_observations"
    }

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
