import pytest
from pandas import Timestamp

from openghg.client import get_obs_surface
from helpers import metadata_checker_obssurface, attributes_checker_get_obs


def test_get_observations_few_args(process_crds):
    result = get_obs_surface(site="hfd", species="co2", inlet="100m")

    metadata = result.metadata

    assert metadata_checker_obssurface(metadata=metadata, species="co2")

    data = result.data

    attrs = data.attrs

    assert attributes_checker_get_obs(attrs=attrs, species="co2")

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert data.time[-1] == Timestamp("2019-05-21T15:46:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf_variability"][0] == pytest.approx(0.109)
    assert data["mf_number_of_observations"][0] == 19.0

    # del data.attrs["File created"]

    expected_attrs = {
        "species": "co2",
        "station_longitude": 0.23048,
        "station_latitude": 50.97675,
        "station_long_name": "Heathfield, UK",
        "station_height_masl": 150.0,
        "site": "hfd",
        "scale": "WMO-X2007",
    }

    for key, value in expected_attrs.items():
        assert attrs[key] == value


def test_get_observations_with_average(process_crds):
    result_no_average = get_obs_surface(site="hfd", species="co2", inlet="100m")

    data_no_average = result_no_average.data

    result = get_obs_surface(site="hfd", species="co2", average="2h", inlet="100m")

    data = result.data

    assert not data["mf"].equals(data_no_average["mf"])

    assert data.time[0] == Timestamp("2013-12-04T14:00:00")
    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(411.08)

    result_with_missing = get_obs_surface(
        site="hfd", species="co2", average="2h", inlet="100m", keep_missing=True
    )

    data_missing = result_with_missing.data

    assert not data_missing.time.equals(data.time)


def test_get_observations_datetime_selection(process_crds):
    results = get_obs_surface(
        site="hfd",
        species="co2",
        inlet="100m",
        start_date="2001-01-01",
        end_date="2015-01-01",
    )

    data = results.data

    assert data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert data.time[-1] == Timestamp("2014-05-07T00:28:30")

    assert data["mf"][0] == pytest.approx(414.21)
    assert data["mf"][-1] == pytest.approx(405.95)


def test_gcwerks_retrieval(process_gcwerks):
    species = "cfc11"
    results = get_obs_surface(site="CGO", species=species, inlet="70m")

    metadata = results.metadata

    assert metadata_checker_obssurface(metadata=metadata, species=species)

    data = results.data
    attrs = data.attrs

    assert attributes_checker_get_obs(attrs=attrs, species=species)

    assert data.time[0] == Timestamp("2018-01-01T02:24:00")
    assert data.time[-1] == Timestamp("2018-01-31T23:33:00")
    assert data["mf"][0] == pytest.approx(226.463)
    assert data["mf"][-1] == pytest.approx(226.017)
    assert data["mf_repeatability"][0] == pytest.approx(0.223)
    assert data["mf_repeatability"][-1] == pytest.approx(0.37784)


def test_get_observations_fixed_dates(process_crds):
    results = get_obs_surface(site="hfd", species="co2", inlet="100m")

    assert results.data.time[0] == Timestamp("2013-12-04T14:02:30")
    assert results.data.time[-1] == Timestamp("2019-05-21T15:46:30")

    start_date = "2015-01-01"
    end_date = "2015-05-31"

    results = get_obs_surface(
        site="hfd",
        species="co2",
        inlet="100m",
        start_date=start_date,
        end_date=end_date,
    )

    assert results.data.time[0] == Timestamp("2015-01-01T18:25:30")
    assert results.data.time[-1] == Timestamp("2015-05-07T00:28:30")

    start_date = Timestamp("2016-01-01")
    end_date = Timestamp("2016-08-01")

    results = get_obs_surface(
        site="hfd",
        species="co2",
        inlet="100m",
        start_date=start_date,
        end_date=end_date,
    )

    assert results.data.time[0] == Timestamp("2016-01-01T18:25:30")
    assert results.data.time[-1] == Timestamp("2016-05-07T00:28:30")
