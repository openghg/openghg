import logging
from pathlib import Path
import pytest

from openghg.modules import ICOS

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


def test_read_file():
    icos = ICOS()

    filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")

    data = icos.read_file(data_filepath=filepath)

    attrs = data["co2"]["data"].attrs

    del attrs["File created"]

    expected_attrs = {'Conditions of use': 'Ensure that you contact the data owner at the outset of your project.', 
                    'Source': 'In situ measurements of air', 'Conventions': 'CF-1.6', 
                    'Processed by': 'OpenGHG_Cloud', 'species': 'co2', 'Calibration_scale': 'unknown', 
                    'station_longitude': -2.98598, 'station_latitude': 56.55511, 'station_long_name': 'Angus Tower, UK', 
                    'station_height_masl': 300.0}

    assert attrs == expected_attrs


def test_read_data():
    icos = ICOS()
    filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")
    data = icos.read_data(data_filepath=filepath, species="CO2")

    co2_data = data["co2"]["data"]

    assert co2_data["co2"][0].values == pytest.approx(401.645)
    assert co2_data["co2 variability"][0].values == pytest.approx(0.087)
    assert co2_data["co2 number_of_observations"][0].values == 13

    assert data["co2"]["metadata"] == {'site': 'tta', 'species': 'co2', 'inlet': '222m', 
                                        'time_resolution': '1minute', 'network': 'ICOS'}
