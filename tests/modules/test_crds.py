import logging
from pathlib import Path

import pandas as pd
import pytest

from openghg.modules import CRDS

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# @pytest.fixture(scope="session")
# def data():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)


#     return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(autouse=True)
def hfd_filepath():
    return get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")


def test_read_file():
    hfd_filepath = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    crds = CRDS()

    gas_data = crds.read_file(data_filepath=hfd_filepath, site="hfd", network="DECC")

    ch4_data = gas_data["ch4"]["data"]
    co2_data = gas_data["co2"]["data"]
    co_data = gas_data["co"]["data"]

    assert ch4_data["ch4"][0].values == pytest.approx(1993.83)
    assert ch4_data["ch4_variability"][0].values == pytest.approx(1.555)
    assert ch4_data["ch4_number_of_observations"][0].values == pytest.approx(19.0)

    assert co2_data["co2"][0] == pytest.approx(414.21)
    assert co2_data["co2_variability"][0] == pytest.approx(0.109)
    assert co2_data["co2_number_of_observations"][0] == pytest.approx(19.0)

    assert co_data["co"][0] == pytest.approx(214.28)
    assert co_data["co_variability"][0] == pytest.approx(4.081)
    assert co_data["co_number_of_observations"][0] == pytest.approx(19.0)


def test_read_file_no_inlet_raises():
    crds = CRDS()
    filepath = Path("tac.picarro.1minute.no_inlet.dat")

    with pytest.raises(ValueError):
        crds.read_file(data_filepath=filepath, site="tac", network="DECC")


def test_gas_info(hfd_filepath):
    crds = CRDS()

    data = pd.read_csv(
        hfd_filepath,
        header=None,
        skiprows=1,
        sep=r"\s+",
        index_col=["0_1"],
        parse_dates=[[0, 1]],
    )

    n_gases, n_cols = crds.gas_info(data=data)

    assert n_gases == 3
    assert n_cols == 3


def test_get_site_attributes():
    crds = CRDS()

    attrs = crds.get_site_attributes(site="bsd", inlet="108m")

    assert attrs == {'data_owner': "Simon O'Doherty", 'data_owner_email': 's.odoherty@bristol.ac.uk', 
                    'inlet_height_magl': '108m', 'comment': 'Cavity ring-down measurements. Output from GCWerks'}

    attrs = crds.get_site_attributes(site="tac", inlet="50m")

    assert attrs == {'data_owner': "Simon O'Doherty", 'data_owner_email': 's.odoherty@bristol.ac.uk', 
                    'inlet_height_magl': '50m', 'comment': 'Cavity ring-down measurements. Output from GCWerks'}


def test_get_site_attributes_unknown_site_raises():
    crds = CRDS()

    with pytest.raises(ValueError):
        _ = crds.get_site_attributes(site="jupiter", inlet="10008m")




