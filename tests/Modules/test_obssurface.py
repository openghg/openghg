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
    precision_filepath = get_datapath(
        filename="capegrim-medusa.18.precisions.C", data_type="GC"
    )

    results = ObsSurface.read_file(
        filepath=(data_filepath, precision_filepath), data_type="GC"
    )

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

    del hfc152a_data.attrs["File created"]

    assert hfc152a_data.attrs == {
        "data_owner": "Paul Krummel",
        "data_owner_email": "paul.krummel@csiro.au",
        "inlet_height_magl": "75m_4",
        "comment": "Medusa measurements. Output from GCWerks. See Miller et al. (2008).",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "auto@hugs-cloud.com",
        "species": "hfc152a",
        "Calibration_scale": "SIO-05",
        "station_longitude": 144.689,
        "station_latitude": -40.683,
        "station_long_name": "Cape Grim, Tasmania",
        "station_height_masl": 94.0,
    }


def test_read_cranfield():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(
        filename="THB_hourly_means_test.csv", data_type="Cranfield_CRDS"
    )

    results = ObsSurface.read_file(filepath=data_filepath, data_type="CRANFIELD")

    expected_keys = sorted(
        [
            "THB_hourly_means_test_ch4",
            "THB_hourly_means_test_co2",
            "THB_hourly_means_test_co",
        ]
    )

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

    # Check obs has stored the keys correctly
    obs = ObsSurface.load()

    assert sorted(list(obs._datasource_names.keys())) == sorted(['THB_hourly_means_test_ch4', 
                                                                'THB_hourly_means_test_co2', 
                                                                'THB_hourly_means_test_co'])

def test_read_icos():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(
        filename="tta.co2.1minute.222m.min.dat", data_type="ICOS"
    )

    results = ObsSurface.read_file(filepath=data_filepath, data_type="ICOS")

    assert list(results["tta.co2.1minute.222m.min.dat"].keys())[0] == "tta.co2.1minute.222m.min_co2"

    uuid = results["tta.co2.1minute.222m.min.dat"]["tta.co2.1minute.222m.min_co2"]

    data = Datasource.load(uuid=uuid, shallow=False).data()

    assert sorted(list(data.keys())) == sorted(
        [
            "2011-12-07-01:38:00+00:00_2011-12-31-19:57:00+00:00",
            "2011-06-01-05:54:00+00:00_2011-08-31-17:58:00+00:00",
            "2011-03-30-08:52:00+00:00_2011-05-31-20:59:00+00:00",
            "2011-09-01-11:20:00+00:00_2011-11-30-03:39:00+00:00",
            "2012-12-01-04:03:00+00:00_2012-12-31-15:41:00+00:00",
            "2012-06-01-11:15:00+00:00_2012-08-07-19:16:00+00:00",
            "2012-04-07-06:20:00+00:00_2012-05-31-18:00:00+00:00",
            "2012-09-05-02:15:00+00:00_2012-11-30-19:08:00+00:00",
            "2013-01-01-00:01:00+00:00_2013-01-17-18:06:00+00:00",
        ]
    )

    co2_data = data["2012-12-01-04:03:00+00:00_2012-12-31-15:41:00+00:00"]

    assert co2_data.time[0] == Timestamp("2012-12-01T04:03:00")
    assert co2_data.time[-1] == Timestamp("2012-12-31T15:41:00")

    assert co2_data["co2"][0] == 397.765
    assert co2_data["co2"][-1] == 398.374

    assert co2_data["co2_variability"][0] == 0.057
    assert co2_data["co2_variability"][-1] == 0.063

    assert co2_data["co2_number_of_observations"][0] == 12
    assert co2_data["co2_number_of_observations"][-1] == 13

    del co2_data.attrs["File created"]

    assert co2_data.attrs == {
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "auto@hugs-cloud.com",
        "species": "co2",
        "Calibration_scale": "unknown",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
    }

    obs = ObsSurface.load()

    assert list(obs._datasource_names.keys())[0] == "tta.co2.1minute.222m.min_co2"


def test_read_noaa():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="NOAA")

    uuid = results["co_pocn25_surface-flask_1_ccgg_event.txt"]["co_pocn25_surface-flask_1_ccgg_event_CO"]

    co_data = Datasource.load(uuid=uuid, shallow=False).data()

    assert len(co_data.keys()) == 95

    old_data = co_data["1990-12-02-12:23:00+00:00_1990-12-02-12:23:00+00:00"]

    assert old_data.time[0] == Timestamp("1990-12-02T12:23:00")
    assert old_data.time[-1] == Timestamp("1990-12-02T12:23:00")

    assert old_data["co"][0] == 141.61
    assert old_data["co"][-1] == 141.61

    assert old_data["co_repeatability"][0] == -999.99
    assert old_data["co_repeatability"][-1] == -999.99

    assert old_data["co_selection_flag"][0] == 0
    assert old_data["co_selection_flag"][-1] == 0

    obs = ObsSurface.load()

    assert list(obs._datasource_names.keys())[0] == "co_pocn25_surface-flask_1_ccgg_event_CO"


def test_read_thames_barrier():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="THAMESBARRIER")

    expected_keys = sorted(['thames_test_20190707_CH4', 'thames_test_20190707_CO2', 'thames_test_20190707_CO'])

    assert sorted(list(results["thames_test_20190707.csv"].keys())) == expected_keys

    uuid = results["thames_test_20190707.csv"]["thames_test_20190707_CO2"]

    data = Datasource.load(uuid=uuid, shallow=False).data()
    data = data["2019-07-01-00:39:55+00:00_2019-08-01-00:10:30+00:00"]

    assert data.time[0] == Timestamp("2019-07-01T00:39:55")
    assert data.time[-1] == Timestamp("2019-08-01T00:10:30")
    assert data["co2"][0] == pytest.approx(417.97344761)
    assert data["co2"][-1] == pytest.approx(417.80000653)
    assert data["co2_variability"][0] == 0
    assert data["co2_variability"][-1] == 0

    obs = ObsSurface.load()

    assert sorted(obs._datasource_names.keys()) == expected_keys

