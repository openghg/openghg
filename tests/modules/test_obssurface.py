import pytest
from pandas import Timestamp

from openghg.modules import Datasource, ObsSurface
from openghg.objectstore import get_local_bucket, exists
from openghg.util import create_daterange_str
from helpers import get_datapath


def test_read_CRDS():
    get_local_bucket(empty=True)

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS", site="bsd", network="DECC")

    keys = results["processed"]["bsd.picarro.1minute.248m.min.dat"].keys()

    assert sorted(keys) == ["ch4", "co", "co2"]

    # Load up the assigned Datasources and check they contain the correct data
    data = results["processed"]["bsd.picarro.1minute.248m.min.dat"]

    ch4_data = Datasource.load(uuid=data["ch4"]).data()
    ch4_data = ch4_data["2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00"]

    assert ch4_data.time[0] == Timestamp("2014-01-30T11:12:30")
    assert ch4_data["ch4"][0] == 1959.55
    assert ch4_data["ch4"][-1] == 1962.8
    assert ch4_data["ch4_variability"][-1] == 1.034
    assert ch4_data["ch4_number_of_observations"][-1] == 26.0

    obs = ObsSurface.load()
    uuid_one = obs.datasources()[0]
    datasource = Datasource.load(uuid=uuid_one)

    data_keys = list(datasource.data().keys())

    expected_keys = [
        "2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
        "2018-02-18-15:42:30+00:00_2018-12-18-15:42:30+00:00",
        "2019-02-03-17:38:30+00:00_2019-12-09-10:47:30+00:00",
        "2020-02-01-18:08:30+00:00_2020-12-01-22:31:30+00:00",
    ]

    assert data_keys == expected_keys

    filepath = get_datapath(filename="bsd.picarro.1minute.248m.future.dat", data_type="CRDS")
    results = ObsSurface.read_file(filepath=filepath, data_type="CRDS", site="bsd", network="DECC")

    uuid_one = obs.datasources()[0]
    datasource = Datasource.load(uuid=uuid_one)
    data_keys = sorted(list(datasource.data().keys()))

    new_expected_keys = [
        "2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
        "2018-02-18-15:42:30+00:00_2018-12-18-15:42:30+00:00",
        "2019-02-03-17:38:30+00:00_2019-12-09-10:47:30+00:00",
        "2020-02-01-18:08:30+00:00_2020-12-01-22:31:30+00:00",
        "2023-01-30-13:56:30+00:00_2023-01-30-14:20:30+00:00",
    ]

    assert data_keys == new_expected_keys

    table = obs._datasource_table
    assert table["bsd"]["decc"]["ch4"]["248m"]
    assert table["bsd"]["decc"]["co2"]["248m"]
    assert table["bsd"]["decc"]["co"]["248m"]


def test_read_GC():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    results = ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="CGO", network="AGAGE")

    expected_keys = [
        "benzene_70m",
        "c4f10_70m",
        "c6f14_70m",
        "ccl4_70m",
        "cf4_70m",
        "cfc112_70m",
        "cfc113_70m",
        "cfc114_70m",
        "cfc115_70m",
        "cfc11_70m",
        "cfc12_70m",
        "cfc13_70m",
        "ch2br2_70m",
        "ch2cl2_70m",
        "ch3br_70m",
        "ch3ccl3_70m",
        "ch3cl_70m",
        "ch3i_70m",
        "chbr3_70m",
        "chcl3_70m",
        "cos_70m",
        "cpropane_70m",
        "desflurane_70m",
        "ethane_70m",
        "ethyne_70m",
        "h1211_70m",
        "h1301_70m",
        "h2402_70m",
        "hcfc124_70m",
        "hcfc132b_70m",
        "hcfc133a_70m",
        "hcfc141b_70m",
        "hcfc142b_70m",
        "hcfc22_70m",
        "hfc125_70m",
        "hfc134a_70m",
        "hfc143a_70m",
        "hfc152a_70m",
        "hfc227ea_70m",
        "hfc236fa_70m",
        "hfc23_70m",
        "hfc245fa_70m",
        "hfc32_70m",
        "hfc365mfc_70m",
        "hfc4310mee_70m",
        "nf3_70m",
        "pce_70m",
        "pfc116_70m",
        "pfc218_70m",
        "pfc318_70m",
        "propane_70m",
        "sf5cf3_70m",
        "sf6_70m",
        "so2f2_70m",
        "tce_70m",
        "toluene_70m",
    ]

    assert sorted(list(results["processed"]["capegrim-medusa.18.C"].keys())) == expected_keys

    # Load in some data
    uuid = results["processed"]["capegrim-medusa.18.C"]["hfc152a_70m"]

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

    assert sorted(obs._datasource_uuids.values()) == expected_keys

    del hfc152a_data.attrs["File created"]

    assert hfc152a_data.attrs == {
        "data_owner": "Paul Krummel",
        "data_owner_email": "paul.krummel@csiro.au",
        "inlet_height_magl": "70m",
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
        "inlet": "70m",
        "sampling_period": "1200",
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

    ObsSurface.read_file(
        filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="THD", instrument="gcmd", network="AGAGE"
    )

    obs = ObsSurface.load()
    table = obs._datasource_table

    assert table["cgo"]["agage"]["nf3"]["70m"]
    assert table["cgo"]["agage"]["hfc236fa"]["70m"]
    assert table["cgo"]["agage"]["h1211"]["70m"]

    assert table["thd"]["agage"]["cfc11"]["10m"]
    assert table["thd"]["agage"]["n2o"]["10m"]
    assert table["thd"]["agage"]["ccl4"]["10m"]


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

def test_read_beaco2n():
    data_filepath = get_datapath(filename="Charlton_Community_Center.csv", data_type="BEACO2N")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="BEACO2N", site="CCC", network="BEACO2N", overwrite=True)

    uuid = results["processed"]["Charlton_Community_Center.csv"]["co2"]

    co2_data = Datasource.load(uuid=uuid, shallow=False).data()
    co2_data = co2_data["2015-04-18-04:00:00+00:00_2015-04-18-10:00:00+00:00"]

    assert co2_data.time[0] == Timestamp("2015-04-18T04:00:00")
    assert co2_data["co2"][0] == 410.4
    assert co2_data["co2_qc"][0] == 2


def test_read_noaa_raw():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="co_pocn25_surface-flask_1_ccgg_event.txt", data_type="NOAA")

    results = ObsSurface.read_file(filepath=data_filepath, data_type="NOAA", site="POCN25", network="NOAA", inlet="flask")

    uuid = results["processed"]["co_pocn25_surface-flask_1_ccgg_event.txt"]["co"]

    co_data = Datasource.load(uuid=uuid, shallow=False).data()

    assert sorted(list(co_data.keys())) == [
        "1990-06-29-05:00:00+00:00_1990-07-10-21:28:00+00:00",
        "2009-06-13-16:32:00+00:00_2009-12-03-00:30:00+00:00",
        "2010-01-10-00:13:00+00:00_2010-12-09-16:05:00+00:00",
        "2011-01-27-04:55:00+00:00_2011-11-11-14:45:00+00:00",
        "2016-12-03-12:37:00+00:00_2016-12-18-05:30:00+00:00",
        "2017-01-27-19:10:00+00:00_2017-07-15-04:15:00+00:00",
    ]

    co_data = co_data["1990-06-29-05:00:00+00:00_1990-07-10-21:28:00+00:00"]

    assert co_data["co"][0] == pytest.approx(94.9)
    assert co_data["co"][-1] == pytest.approx(95.65)

    assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
    assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)

    assert co_data["co_selection_flag"][0] == 0
    assert co_data["co_selection_flag"][-1] == 0


def test_read_noaa_obspack():
    data_filepath = get_datapath(filename="ch4_esp_surface-flask_2_representative.nc", data_type="NOAA")

    results = ObsSurface.read_file(
        filepath=data_filepath, inlet="flask", data_type="NOAA", site="esp", network="NOAA", overwrite=True
    )

    uuid = results["processed"]["ch4_esp_surface-flask_2_representative.nc"]["ch4"]

    ch4_data = Datasource.load(uuid=uuid, shallow=False).data()

    assert sorted(list(ch4_data.keys())) == [
        "1993-06-17-00:12:30+00:00_1993-11-20-21:50:00+00:00",
        "1994-01-02-22:10:00+00:00_1994-12-24-22:15:00+00:00",
        "1995-02-06-12:00:00+00:00_1995-11-08-19:55:00+00:00",
        "1996-01-21-22:10:00+00:00_1996-12-01-20:00:00+00:00",
        "1997-02-12-19:00:00+00:00_1997-12-20-20:15:00+00:00",
        "1998-01-01-23:10:00+00:00_1998-12-31-19:50:00+00:00",
        "1999-01-14-22:15:00+00:00_1999-12-31-23:35:00+00:00",
        "2000-03-05-00:00:00+00:00_2000-11-04-22:30:00+00:00",
        "2001-01-05-21:45:00+00:00_2001-12-06-12:00:00+00:00",
        "2002-01-12-12:00:00+00:00_2002-01-12-12:00:00+00:00",
    ]

    data = ch4_data["1998-01-01-23:10:00+00:00_1998-12-31-19:50:00+00:00"]

    assert data.time[0] == Timestamp("1998-01-01T23:10:00")
    assert data["value"][0] == pytest.approx(1.83337e-06)
    assert data["nvalue"][0] == 2.0
    assert data["value_std_dev"][0] == pytest.approx(2.093036e-09)


def test_read_thames_barrier():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    results = ObsSurface.read_file(
        filepath=data_filepath, data_type="THAMESBARRIER", site="TMB", network="LGHG", sampling_period=3600
    )

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

    assert sorted(obs._datasource_uuids.values()) == expected_keys


def test_upload_same_file_twice_raises():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    ObsSurface.read_file(filepath=data_filepath, data_type="THAMESBARRIER", site="tmb", network="LGHG", sampling_period=60)

    # assert not res["error"]

    with pytest.raises(ValueError):
        ObsSurface.read_file(filepath=data_filepath, data_type="THAMESBARRIER", site="tmb", network="LGHG", sampling_period=60)

    # assert "tta.co2.1minute.222m.min.dat" in res["error"]


def test_delete_Datasource():
    bucket = get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="thames_test_20190707.csv", data_type="THAMESBARRIER")

    ObsSurface.read_file(filepath=data_filepath, data_type="THAMESBARRIER", site="tmb", network="LGHG", sampling_period=60)

    obs = ObsSurface.load()

    datasources = obs.datasources()

    uuid = datasources[0]

    datasource = Datasource.load(uuid=uuid)

    data_keys = datasource.data_keys()

    key = data_keys[0]

    assert exists(bucket=bucket, key=key)

    obs.delete(uuid=uuid)

    assert uuid not in obs.datasources()

    assert not exists(bucket=bucket, key=key)


def test_add_new_data_correct_datasource():
    get_local_bucket(empty=True)

    data_filepath = get_datapath(filename="capegrim-medusa.05.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.05.precisions.C", data_type="GC")

    results = ObsSurface.read_file(filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="CGO", network="AGAGE")

    first_results = results["processed"]["capegrim-medusa.05.C"]

    sorted_keys = sorted(list(results["processed"]["capegrim-medusa.05.C"].keys()))

    assert sorted_keys[:4] == ["benzene_10m", "benzene_70m", "ccl4_10m", "ccl4_70m"]
    assert sorted_keys[-4:] == ["so2f2_10m", "so2f2_70m", "toluene_10m", "toluene_70m"]
    assert len(sorted_keys) == 69

    data_filepath = get_datapath(filename="capegrim-medusa.06.C", data_type="GC")
    precision_filepath = get_datapath(filename="capegrim-medusa.06.precisions.C", data_type="GC")

    new_results = ObsSurface.read_file(
        filepath=(data_filepath, precision_filepath), data_type="GCWERKS", site="CGO", network="AGAGE"
    )

    second_results = new_results["processed"]["capegrim-medusa.06.C"]

    shared_keys = [key for key in first_results if key in second_results]

    assert len(shared_keys) == 67

    for key in shared_keys:
        assert first_results[key] == second_results[key]


def test_set_rank():
    o = ObsSurface.load()

    o._rank_data.clear()

    test_uid = "test-uid-123"

    daterange_str = create_daterange_str(start="2001-01-01", end="2005-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data == {"test-uid-123": {"2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1}}

    daterange_str = create_daterange_str(start="2007-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {
        "2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1,
        "2007-01-01-00:00:00+00:00_2009-01-01-00:00:00+00:00": 1,
    }

    # Make sure we can't set another rank for the same daterange
    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str)

    daterange_str = create_daterange_str(start="2008-01-01", end="2009-01-01")

    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=3, date_range=daterange_str)

    daterange_str = create_daterange_str(start="2007-01-01", end="2015-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {
        "2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1,
        "2007-01-01-00:00:00+00:00_2015-01-01-00:00:00+00:00": 1,
    }


def test_set_rank_overwrite():
    o = ObsSurface.load()

    o._rank_data.clear()

    test_uid = "test-uid-123"

    daterange_str = create_daterange_str(start="2007-01-01", end="2015-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)
    assert o._rank_data["test-uid-123"] == {"2007-01-01-00:00:00+00:00_2015-01-01-00:00:00+00:00": 1}

    daterange_str = create_daterange_str(start="2008-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    expected_ranking = {
        "2007-01-01-00:00:00+00:00_2007-12-31-23:59:59+00:00": 1,
        "2008-01-01-00:00:00+00:00_2008-12-31-23:59:59+00:00": 2,
        "2009-01-01-00:00:01+00:00_2015-01-01-00:00:00+00:00": 1,
    }

    assert o._rank_data["test-uid-123"] == expected_ranking

    daterange_str = create_daterange_str(start="1994-01-01", end="2023-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    assert o._rank_data["test-uid-123"] == {"1994-01-01-00:00:00+00:00_2023-01-01-00:00:00+00:00": 2}

    o._rank_data.clear()

    daterange_str = create_daterange_str(start="2001-01-01", end="2021-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {"2001-01-01-00:00:00+00:00_2021-01-01-00:00:00+00:00": 1}

    daterange_str = create_daterange_str(start="2007-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    daterange_str = create_daterange_str(start="2015-01-01", end="2016-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    expected = {
        "2001-01-01-00:00:00+00:00_2006-12-31-23:59:59+00:00": 1,
        "2007-01-01-00:00:00+00:00_2008-12-31-23:59:59+00:00": 2,
        "2009-01-01-00:00:01+00:00_2014-12-31-23:59:59+00:00": 1,
        "2015-01-01-00:00:00+00:00_2015-12-31-23:59:59+00:00": 2,
        "2016-01-01-00:00:01+00:00_2021-01-01-00:00:00+00:00": 1,
    }

    assert o._rank_data["test-uid-123"] == expected


def test_rank_overlapping_dateranges():
    dateranges = ["2014-01-01_2099-06-06", "2014-06-07_2015-09-09", "2015-09-10_2019-01-06"]

    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range=dateranges)

    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=2, date_range=dateranges)


def test_rank_same_daterange_doesnt_change():
    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2012-06-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 1}}

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2012-06-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 1}}


def test_rank_daterange_start_overlap_overwrite():
    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2013-01-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2013-01-01-00:00:00+00:00": 1}}

    o.set_rank(uuid=test_uid, rank=2, date_range="2012-01-01_2012-06-01", overwrite=True)

    assert o._rank_data == {
        "test-uid-123": {
            "2012-06-01-00:00:01+00:00_2013-01-01-00:00:00+00:00": 1,
            "2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 2,
        }
    }

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2013-01-01", overwrite=True)

    expected = {"test-uid-123": {"2012-01-01-00:00:00+00:00_2013-01-01-00:00:00+00:00": 1}}

    assert o._rank_data == expected
