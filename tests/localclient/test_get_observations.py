import pytest
from pandas import Timestamp
from pathlib import Path

from openghg.localclient import get_single_site, scale_convert
from openghg.modules import ObsSurface
from openghg.objectstore import get_local_bucket


def get_datapath(filename, data_type):
    return (
        Path(__file__)
        .resolve(strict=True)
        .parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")
    )


@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)

    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = get_datapath(filename=filename, data_type="CRDS")

    ObsSurface.read_file(filepath=filepath, data_type="CRDS")


def test_get_single_site_few_args():
    result = get_single_site(site="hfd", species="co2")

    data = result[0]

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["co2_stdev"][0] == pytest.approx(0.109)
    assert data["co2_n_meas"][0] == 19.0

    del data.attrs["File created"]

    expected_attrs = {
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "100m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "auto@hugs-cloud.com",
        "species": "CO2",
        "station_longitude": 0.23048,
        "station_latitude": 50.97675,
        "station_long_name": "Heathfield, UK",
        "station_height_masl": 150.0,
        "scale": "WMO-X2007",
    }

    assert data.attrs == expected_attrs 


def test_get_single_site_with_average():
    result_no_average = get_single_site(site="hfd", species="co2")

    data_no_average = result_no_average[0]

    result = get_single_site(site="hfd", species="co2", average="2h")

    data = result[0]

    assert not data["mf"].equals(data_no_average["mf"])

    assert data.time[0] == Timestamp("2013-12-04T14:00:00")
    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(411.08)

    result_with_missing = get_single_site(site="hfd", species="co2", average="2h", keep_missing=True)

    data_missing = result_with_missing[0]

    assert data_missing.time.equals(data.time)


def test_get_single_site_datetime_selection():
    results = get_single_site(site="hfd", species="co2", start_date="2001-01-01", end_date="2015-01-01")

    data = results[0]

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert data.time[-1] == Timestamp("2014-12-28T08:02:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(407.55)


def test_scale_convert():
    results = get_single_site(site="hfd", species="co", start_date="2001-01-01", end_date="2015-01-01")

    data = results[0]

    assert data["mf"][0] == pytest.approx(214.28)
    assert data["co_stdev"][0] == pytest.approx(4.081)
    assert data["co_n_meas"][0] == 19.0

    # # Fix the scale for test purposes
    data.attrs["scale"] = "WMO-X2014A"

    new_scale = "CSIRO94"

    data = scale_convert(data=data, species="co", to_scale=new_scale)

    assert data["mf"][0] == pytest.approx(902.755)
    assert data["co_stdev"][0] == pytest.approx(4.081)
    assert data["co_n_meas"][0] == 19.0

    assert data.attrs["scale"] == new_scale

    results = get_single_site(site="hfd", species="ch4", start_date="2001-01-01", end_date="2015-01-01")

    data = results[0]

    assert data["mf"][0] == pytest.approx(1993.83)
    assert data["ch4_stdev"][0] == pytest.approx(1.555)
    assert data["ch4_n_meas"][0] == 19.0

    data.attrs["scale"] = "WMO-X2014A"

    data = scale_convert(data=data, species="co", to_scale=new_scale)

    assert data["mf"][0] == pytest.approx(8399.95)
    assert data["ch4_stdev"][0] == pytest.approx(1.555)
    assert data["ch4_n_meas"][0] == 19.0
