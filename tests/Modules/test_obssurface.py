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
        .parent.parent.joinpath("data", "proc_test_data", data_type, filename)
    )


def test_read_CRDS():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.dat", data_type="CRDS")

    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS")

    keys = results["bsd.picarro.1minute.248m.dat"].keys()

    expected_keys = sorted(
        [
            "bsd.picarro.1minute.248m_ch4",
            "bsd.picarro.1minute.248m_co",
            "bsd.picarro.1minute.248m_co2",
        ]
    )
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


def test_read_GC():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    results = ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GC")

    expected_keys = sorted(
        [
            "capegrim-medusa.18_NF3",
            "capegrim-medusa.18_CF4",
            "capegrim-medusa.18_PFC-116",
            "capegrim-medusa.18_PFC-218",
            "capegrim-medusa.18_PFC-318",
            "capegrim-medusa.18_C4F10",
            "capegrim-medusa.18_C6F14",
            "capegrim-medusa.18_SF6",
            "capegrim-medusa.18_SO2F2",
            "capegrim-medusa.18_SF5CF3",
            "capegrim-medusa.18_HFC-23",
            "capegrim-medusa.18_HFC-32",
            "capegrim-medusa.18_HFC-125",
            "capegrim-medusa.18_HFC-134a",
            "capegrim-medusa.18_HFC-143a",
            "capegrim-medusa.18_HFC-152a",
            "capegrim-medusa.18_HFC-227ea",
            "capegrim-medusa.18_HFC-236fa",
            "capegrim-medusa.18_HFC-245fa",
            "capegrim-medusa.18_HFC-365mfc",
            "capegrim-medusa.18_HFC-4310mee",
            "capegrim-medusa.18_HCFC-22",
            "capegrim-medusa.18_HCFC-124",
            "capegrim-medusa.18_HCFC-132b",
            "capegrim-medusa.18_HCFC-133a",
            "capegrim-medusa.18_HCFC-141b",
            "capegrim-medusa.18_HCFC-142b",
            "capegrim-medusa.18_CFC-11",
            "capegrim-medusa.18_CFC-12",
            "capegrim-medusa.18_CFC-13",
            "capegrim-medusa.18_CFC-112",
            "capegrim-medusa.18_CFC-113",
            "capegrim-medusa.18_CFC-114",
            "capegrim-medusa.18_CFC-115",
            "capegrim-medusa.18_H-1211",
            "capegrim-medusa.18_H-1301",
            "capegrim-medusa.18_H-2402",
            "capegrim-medusa.18_CH3Cl",
            "capegrim-medusa.18_CH3Br",
            "capegrim-medusa.18_CH3I",
            "capegrim-medusa.18_CH2Cl2",
            "capegrim-medusa.18_CHCl3",
            "capegrim-medusa.18_CCl4",
            "capegrim-medusa.18_CH2Br2",
            "capegrim-medusa.18_CHBr3",
            "capegrim-medusa.18_CH3CCl3",
            "capegrim-medusa.18_TCE",
            "capegrim-medusa.18_PCE",
            "capegrim-medusa.18_ethyne",
            "capegrim-medusa.18_ethane",
            "capegrim-medusa.18_propane",
            "capegrim-medusa.18_c-propane",
            "capegrim-medusa.18_benzene",
            "capegrim-medusa.18_toluene",
            "capegrim-medusa.18_COS",
            "capegrim-medusa.18_desflurane",
        ]
    )

    sorted(list(results["capegrim-medusa.18.C"].keys())) == expected_keys

    # Load in some data
    uuid = results["capegrim-medusa.18.C"]["capegrim-medusa.18_HFC-152a"]

    hfc152a_data = Datasource.load(uuid=uuid, shallow=False).data()
    hfc152a_data = hfc152a_data["2018-01-01-02:24:00+00:00_2018-01-31-23:33:00+00:00"]

    assert hfc152a_data.time[0] == Timestamp("2018-01-01T02:24:00")
    assert hfc152a_data.time[-1] == Timestamp("2018-01-31T23:33:00")

    assert hfc152a_data["hfc152a"][0] == 4.409
    assert hfc152a_data["hfc152a"][-1] == 4.262

    assert hfc152a_data["hfc152a_repeatability"][0] == 0.03557
    assert hfc152a_data["hfc152a_repeatability"][-1] == 0.03271

    assert hfc152a_data["hfc152a_status_flag"][0] == 0
    assert hfc152a_data["hfc152a_status_flag"][-1] == 0

    assert hfc152a_data["hfc152a_integration_flag"][0] == 0
    assert hfc152a_data["hfc152a_integration_flag"][-1] == 0

    # Check we have the Datasource info saved

    obs = ObsSurface.load()

    assert sorted(obs._datasource_names.keys()) == expected_keys


def test_read_cranfield():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="THB_hourly_means_test.csv", data_type="Cranfield_CRDS")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="Cranfield")

    expected_keys = sorted(['THB_hourly_means_test_ch4', 'THB_hourly_means_test_co2', 'THB_hourly_means_test_co'])

    assert sorted(results["THB_hourly_means_test.csv"].keys()) == expected_keys

    uuid = results["THB_hourly_means_test.csv"]["THB_hourly_means_test_ch4"]

    ch4_data = Datasource.load(uuid=uuid, shallow=False).data()
    ch4_data = ch4_data["2018-05-05-00:00:00+00:00_2018-05-13-16:00:00+00:00"]

    assert ch4_data.time[0] == Timestamp("2018-05-05")
    assert ch4_data.time[-1] == Timestamp("2018-05-13T16:00:00")

    assert ch4_data["ch4"][0] == pytest.approx(2585.651)
    assert ch4_data["ch4"][-1] == pytest.approx(1999.018)

    assert ch4_data["ch4 variability"][0] == pytest.approx(75.50218)
    assert ch4_data["ch4 variability"][-1] == pytest.approx(6.48413)


