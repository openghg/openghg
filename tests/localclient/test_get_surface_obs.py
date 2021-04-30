import pytest
from pandas import Timestamp
from pathlib import Path

from openghg.localclient import get_obs_surface
from openghg.modules import ObsSurface
from openghg.objectstore import get_local_bucket


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(scope="session", autouse=True)
def read_data():
    get_local_bucket(empty=True)

    hfd_filepath = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    cgo_data = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    cgo_prec = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=hfd_filepath, data_type="CRDS", site="hfd", network="DECC", inlet="100m")
    ObsSurface.read_file(filepath=(cgo_data, cgo_prec), data_type="GCWERKS", site="CGO", network="AGAGE", instrument="medusa", inlet="75m")


def test_get_observations_few_args():
    result = get_obs_surface(site="hfd", species="co2", inlet="100m")

    data = result.data

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert data.time[-1] == Timestamp("2019-05-21T15:46:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf_variability"][0] == pytest.approx(0.109)
    assert data["mf_number_of_observations"][0] == 19.0

    del data.attrs["File created"]

    expected_attrs = {
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "100m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "co2",
        "station_longitude": 0.23048,
        "station_latitude": 50.97675,
        "station_long_name": "Heathfield, UK",
        "station_height_masl": 150.0,
        "site": "hfd",
        "instrument": "picarro",
        "sampling_period": 60,
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "scale": "WMO-X2007",
        "network": "decc",
    }

    assert data.attrs == expected_attrs


def test_get_observations_with_average():
    result_no_average = get_obs_surface(site="hfd", species="co2")

    data_no_average = result_no_average.data

    result = get_obs_surface(site="hfd", species="co2", average="2h")

    data = result.data

    assert not data["mf"].equals(data_no_average["mf"])

    assert data.time[0] == Timestamp("2013-12-04T14:00:00")
    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(411.08)

    result_with_missing = get_obs_surface(site="hfd", species="co2", average="2h", keep_missing=True)

    data_missing = result_with_missing.data

    assert not data_missing.time.equals(data.time)


def test_get_observations_datetime_selection():
    results = get_obs_surface(site="hfd", species="co2", start_date="2001-01-01", end_date="2015-01-01")

    data = results.data

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert data.time[-1] == Timestamp("2014-05-07T00:28:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(405.95)


def test_gcwerks_retrieval():
    results = get_obs_surface(site="CGO", species="cfc11")

    data = results.data
    metadata = results.metadata

    del metadata["File created"]

    expected_metadata = {
        "data_owner": "Paul Krummel",
        "data_owner_email": "paul.krummel@csiro.au",
        "inlet_height_magl": "70m",
        "comment": "Medusa measurements. Output from GCWerks. See Miller et al. (2008).",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "cfc11",
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
        "sampling_period": 1200,
    }

    assert metadata == expected_metadata

    assert data.time[0] == Timestamp("2018-01-01T02:24:00")
    assert data.time[-1] == Timestamp("2018-01-31T23:33:00")
    assert data["mf"][0] == pytest.approx(226.463)
    assert data["mf"][-1] == pytest.approx(226.017)
    assert data["mf_repeatability"][0] == pytest.approx(0.223)
    assert data["mf_repeatability"][-1] == pytest.approx(0.37784)


def test_get_observations_fixed_dates():
    results = get_obs_surface(site="hfd", species="co2", inlet="100m")

    assert results.data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert results.data.time[-1] == Timestamp("2019-05-21T15:46:30")

    start_date = "2015-01-01"
    end_date = "2015-05-31"

    results = get_obs_surface(site="hfd", species="co2", inlet="100m", start_date=start_date, end_date=end_date)

    assert results.data.time[0] == Timestamp("2015-01-01T18:25:30")
    assert results.data.time[-1] == Timestamp("2015-05-07T00:28:30")

    start_date = Timestamp("2016-01-01")
    end_date = Timestamp("2016-08-01")

    results = get_obs_surface(site="hfd", species="co2", inlet="100m", start_date=start_date, end_date=end_date)

    assert results.data.time[0] == Timestamp("2016-01-01T18:25:30")
    assert results.data.time[-1] == Timestamp("2016-05-07T00:28:30")





    
