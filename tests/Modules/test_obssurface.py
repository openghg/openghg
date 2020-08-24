import pytest
from pandas import Timestamp
from pathlib import Path

from HUGS.Modules import Datasource, ObsSurface
from HUGS.ObjectStore import get_local_bucket


def get_datapath(filename, data_type):
    """ Get the path of a file in the tests directory 

        Returns:
            pathlib.Path
    """
    return (
        Path(__file__)
        .resolve()
        .parent.parent.joinpath(
            "data", "proc_test_data", data_type.upper(), filename
        )
    )


def test_read_CRDS_file():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.dat", data_type="CRDS")

    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS")

    keys = results["bsd.picarro.1minute.248m.dat"].keys()

    expected_keys = sorted(["bsd.picarro.1minute.248m_ch4", "bsd.picarro.1minute.248m_co", "bsd.picarro.1minute.248m_co2"])
    assert sorted(keys) == expected_keys

    # Load up the assigned Datasources and check they contain the correct data
    data = results["bsd.picarro.1minute.248m.dat"]

    ch4_data = Datasource.load(uuid=data["bsd.picarro.1minute.248m_ch4"]).data()
    ch4_data = ch4_data["2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"]

    assert ch4_data.time[0] == Timestamp("2014-01-30T10:52:30")
    assert ch4_data["ch4"][0] == 1960.24
    assert ch4_data["ch4"][-1] == 1952.24
    assert ch4_data["ch4_stdev"][-1] == 0.674
    assert ch4_data["ch4_n_meas"][-1] == 25.0

    obs = ObsSurface.load()

    assert sorted(obs._datasource_names.keys()) == expected_keys


def test_read_GC_file():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.dat", data_type="CRDS")