import pytest
from pandas import Timestamp
from pathlib import Path

from openghg.modules import Datasource, ObsSurface
from openghg.objectstore import get_local_bucket, exists


def get_datapath(filename, data_type):
    """Get the path of a file in the tests directory

    Returns:
        pathlib.Path
    """
    return Path(__file__).resolve().parent.parent.joinpath("data", "proc_test_data", data_type, filename)


def test_read_CRDS():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.dat", data_type="CRDS")
    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS", site="bsd", network="DECC")

    keys = results["processed"]["bsd.picarro.1minute.248m.dat"].keys()

    assert sorted(keys) == ["ch4", "co", "co2"]

    # Load up the assigned Datasources and check they contain the correct data
    data = results["processed"]["bsd.picarro.1minute.248m.dat"]

    ch4_data = Datasource.load(uuid=data["ch4"]).data()
    ch4_data = ch4_data["2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"]

    assert ch4_data.time[0] == Timestamp("2014-01-30T10:52:30")
    assert ch4_data["ch4"][0] == 1960.24
    assert ch4_data["ch4"][-1] == 1952.24
    assert ch4_data["ch4_variability"][-1] == 0.674
    assert ch4_data["ch4_number_of_observations"][-1] == 25.0

    obs = ObsSurface.load()
    uuid_one = obs.datasources()[0]
    datasource = Datasource.load(uuid=uuid_one)

    data_keys = list(datasource.data().keys())

    assert data_keys == ['2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00', '2018-01-30-13:52:30+00:00_2018-01-30-14:20:30+00:00']

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.future.dat", data_type="CRDS")
    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS", site="bsd", network="DECC")

    uuid_one = obs.datasources()[0]
    datasource = Datasource.load(uuid=uuid_one)
    data_keys = sorted(list(datasource.data().keys()))

    assert data_keys == [
        "2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00",
        '2018-01-30-13:52:30+00:00_2018-01-30-14:20:30+00:00',
        "2023-01-30-13:56:30+00:00_2023-01-30-14:20:30+00:00",
    ]

    table = obs._datasource_table

    assert table["bsd"]["decc"]["248m"]["ch4"]
    assert table["bsd"]["decc"]["248m"]["co2"]
    assert table["bsd"]["decc"]["248m"]["co"]


def test_read_GC():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    results = ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="CGO", network="AGAGE")

    expected_keys = [
        "benzene_75m_4",
        "c4f10_75m_4",
        "c6f14_75m_4",
        "ccl4_75m_4",
        "cf4_75m_4",
        "cfc112_75m_4",
        "cfc113_75m_4",
        "cfc114_75m_4",
        "cfc115_75m_4",
        "cfc11_75m_4",
        "cfc12_75m_4",
        "cfc13_75m_4",
        "ch2br2_75m_4",
        "ch2cl2_75m_4",
        "ch3br_75m_4",
        "ch3ccl3_75m_4",
        "ch3cl_75m_4",
        "ch3i_75m_4",
        "chbr3_75m_4",
        "chcl3_75m_4",
        "cos_75m_4",
        "cpropane_75m_4",
        "desflurane_75m_4",
        "ethane_75m_4",
        "ethyne_75m_4",
        "h1211_75m_4",
        "h1301_75m_4",
        "h2402_75m_4",
        "hcfc124_75m_4",
        "hcfc132b_75m_4",
        "hcfc133a_75m_4",
        "hcfc141b_75m_4",
        "hcfc142b_75m_4",
        "hcfc22_75m_4",
        "hfc125_75m_4",
        "hfc134a_75m_4",
        "hfc143a_75m_4",
        "hfc152a_75m_4",
        "hfc227ea_75m_4",
        "hfc236fa_75m_4",
        "hfc23_75m_4",
        "hfc245fa_75m_4",
        "hfc32_75m_4",
        "hfc365mfc_75m_4",
        "hfc4310mee_75m_4",
        "nf3_75m_4",
        "pce_75m_4",
        "pfc116_75m_4",
        "pfc218_75m_4",
        "pfc318_75m_4",
        "propane_75m_4",
        "sf5cf3_75m_4",
        "sf6_75m_4",
        "so2f2_75m_4",
        "tce_75m_4",
        "toluene_75m_4",
    ]

    assert sorted(list(results["processed"]["capegrim-medusa.18.C"].keys())) == expected_keys

    # Load in some data
    uuid = results["processed"]["capegrim-medusa.18.C"]["hfc152a_75m_4"]

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
        "Processed by": "OpenGHG_Cloud",
        "species": "hfc152a",
        "Calibration_scale": "SIO-05",
        "station_longitude": 144.689,
        "station_latitude": -40.683,
        "station_long_name": "Cape Grim, Tasmania",
        "station_height_masl": 94.0,
        "instrument": "medusa",
        "site": "cgo",
        "network": "agage",
        "units": "ppt",
        "scale": "SIO-05",
        "inlet": "75m_4",
    }

    # # Now test that if we add more data it adds it to the same Datasource
    uuid_one = obs.datasources()[0]

    datasource = Datasource.load(uuid=uuid_one)

    data_one = datasource.data()
    assert list(data_one.keys()) == ["2018-01-01-02:24:00+00:00_2018-01-31-23:33:00+00:00"]

    data_filepath = get_datapath(filename="capegrim-medusa.future.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.future.precisions.C", data_type="GC")

    results = ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="CGO", network="AGAGE")

    datasource = Datasource.load(uuid=uuid_one)
    data_one = datasource.data()

    assert sorted(list(data_one.keys())) == [
        "2018-01-01-02:24:00+00:00_2018-01-31-23:33:00+00:00",
        "2023-01-01-02:24:00+00:00_2023-01-31-23:33:00+00:00",
    ]

    data_filepath = get_datapath(filename="trinidadhead.01.C", data_type="GC")
    precision_filepath = get_datapath(filename="trinidadhead.01.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="THD", network="AGAGE")

    obs = ObsSurface.load()
    table = obs._datasource_table

    assert table["cgo"]["agage"]["75m_4"]["nf3"]
    assert table["cgo"]["agage"]["75m_4"]["hfc236fa"]
    assert table["cgo"]["agage"]["75m_4"]["h1211"]

    assert table["thd"]["agage"]["any"]["cfc11"]
    assert table["thd"]["agage"]["any"]["n2o"]
    assert table["thd"]["agage"]["any"]["ccl4"]


def test_read_cranfield():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="THB_hourly_means_test.csv", data_type="Cranfield_CRDS")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="CRANFIELD", site="THB", network="CRANFIELD")

    expected_keys = ["ch4", "co", "co2"]

    assert sorted(results["processed"]["THB_hourly_means_test.csv"].keys()) == expected_keys

    uuid = results["processed"]["THB_hourly_means_test.csv"]["ch4"]

    ch4_data = Datasource.load(uuid=uuid, shallow=False).data()
    ch4_data = ch4_data["2018-05-05-00:00:00+00:00_2018-05-13-16:00:00+00:00"]

    assert ch4_data.time[0] == Timestamp("2018-05-05")
    assert ch4_data.time[-1] == Timestamp("2018-05-13T16:00:00")

    assert ch4_data["ch4"][0] == pytest.approx(2585.651)
    assert ch4_data["ch4"][-1] == pytest.approx(1999.018)

    assert ch4_data["ch4 variability"][0] == pytest.approx(75.50218)
    assert ch4_data["ch4 variability"][-1] == pytest.approx(6.48413)


def test_read_icos():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="ICOS", site="tta", network="ICOS")

    uuid = results["processed"]["tta.co2.1minute.222m.min.dat"]["co2"]

    data = Datasource.load(uuid=uuid, shallow=False).data()

    assert sorted(list(data.keys())) == sorted(
        ["2011-03-30-08:52:00+00:00_2011-04-10-16:06:00+00:00", "2013-01-09-17:49:00+00:00_2013-01-17-18:06:00+00:00"]
    )

    co2_data = data["2011-03-30-08:52:00+00:00_2011-04-10-16:06:00+00:00"]

    assert co2_data.time[0] == Timestamp("2011-03-30T08:52:00")
    assert co2_data.time[-1] == Timestamp("2011-04-10T16:06:00")
    assert co2_data["co2"][0] == pytest.approx(401.645)
    assert co2_data["co2"][-1] == pytest.approx(391.443)
    assert co2_data["co2_variability"][0] == pytest.approx(0.087)
    assert co2_data["co2_variability"][-1] == pytest.approx(0.048)
    assert co2_data["co2_number_of_observations"][0] == 13
    assert co2_data["co2_number_of_observations"][-1] == 13

    del co2_data.attrs["File created"]

    assert co2_data.attrs == {
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "co2",
        "Calibration_scale": "unknown",
        "station_longitude": -2.98598,
        "station_latitude": 56.55511,
        "station_long_name": "Angus Tower, UK",
        "station_height_masl": 300.0,
        "site": "tta",
        "inlet": "222m",
        "time_resolution": "1minute",
        "network": "ICOS",
    }

    obs = ObsSurface.load()

    assert list(obs._datasource_names.keys())[0] == "co2"


def test_read_beaco2n():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="Charlton_Community_Center.csv", data_type="BEACO2N")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="BEACO2N", site="CCC", network="BEACO2N")

    uuid = results["processed"]["Charlton_Community_Center.csv"]["co2"]

    co2_data = Datasource.load(uuid=uuid, shallow=False).data()
    co2_data = co2_data["2015-04-18-04:00:00+00:00_2015-04-18-10:00:00+00:00"]

    assert co2_data.time[0] == Timestamp("2015-04-18T04:00:00")
    assert co2_data["co2"][0] == 410.4
    assert co2_data["co2_qc"][0] == 2


def test_read_noaa():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="NOAA", site="POCN25", network="NOAA")

    uuid = results["processed"]["co_pocn25_surface-flask_1_ccgg_event.txt"]["co"]

    co_data = Datasource.load(uuid=uuid, shallow=False).data()

    assert len(co_data.keys()) == 16

    co_data = co_data["1990-06-29-05:00:00+00:00_1990-07-10-21:28:00+00:00"]

    assert co_data["co"][0] == pytest.approx(94.9)
    assert co_data["co"][-1] == pytest.approx(95.65)

    assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
    assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)

    assert co_data["co_selection_flag"][0] == 0
    assert co_data["co_selection_flag"][-1] == 0


def test_read_thames_barrier():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="THAMESBARRIER", site="TMB", network="LGHG")

    expected_keys = sorted(["CH4", "CO2", "CO"])

    assert sorted(list(results["processed"]["thames_test_20190707.csv"].keys())) == expected_keys

    uuid = results["processed"]["thames_test_20190707.csv"]["CO2"]

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


def test_upload_same_file_twice_raises():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")

    res = ObsSurface.read_file(filepath=data_filepath, data_type="ICOS", site="tta", network="ICOS")

    assert not res["error"] 

    res = ObsSurface.read_file(filepath=data_filepath, data_type="ICOS", site="tta", network="ICOS")

    assert "tta.co2.1minute.222m.min.dat" in res["error"]


def test_delete_Datasource():
    bucket = get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="tta.co2.1minute.222m.min.dat", data_type="ICOS")

    ObsSurface.read_file(filepath=data_filepath, data_type="ICOS", site="tta", network="ICOS")

    obs = ObsSurface.load()

    datasources = obs.datasources()

    uuid = datasources[0]

    datasource = Datasource.load(uuid=uuid)

    data = datasource.data()["2011-03-30-08:52:00+00:00_2011-04-10-16:06:00+00:00"]

    assert data["co2"][0] == pytest.approx(401.645)
    assert data.time[0] == Timestamp("2011-03-30T08:52:00")

    data_keys = datasource.data_keys()

    key = data_keys[0]

    assert exists(bucket=bucket, key=key)

    obs.delete(uuid=uuid)

    assert uuid not in obs.datasources()

    assert not exists(bucket=bucket, key=key)
