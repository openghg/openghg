from typing import Optional

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.dataobjects import ObsData
from openghg.standardise import standardise_surface
from openghg.retrieve import search_surface
from openghg.util import combine_and_elevate_inlet, combine_data_objects, combine_multisite

from helpers import clear_test_stores, get_surface_datapath


def make_obs_dataset(start: str, end: str) -> xr.Dataset:
    times = pd.date_range(start, end, inclusive="left")  # daily frequency
    values = np.arange(0, len(times))

    return xr.Dataset({"mf": (["time"], values)}, coords={"time": times})


def make_obs_data_object(start: str, end: str, metadata: Optional[dict] = None) -> ObsData:
    data = make_obs_dataset(start, end)

    if metadata is None:
        metadata = {}

    obs_dat = ObsData(metadata=metadata, data=data)

    return obs_dat


@pytest.fixture()
def make_obs_data():
    obs_dat1 = make_obs_data_object("2024-01-01", "2024-02-01", metadata={"test": 1, "inlet": "10m"})
    obs_dat2 = make_obs_data_object("2024-02-01", "2024-03-01", metadata={"test": 2, "inlet": "11m"})
    obs_dat3 = make_obs_data_object("2024-03-01", "2024-04-01", metadata={"test": 3, "inlet": "12m"})
    obs_dat4 = make_obs_data_object("2024-04-01", "2024-05-01", metadata={"test": 4, "inlet": "10m"})

    return [obs_dat1, obs_dat2, obs_dat3, obs_dat4]


def test_combine_data_objects(make_obs_data):
    data_objects = make_obs_data
    result = combine_data_objects(data_objects)

    assert result.metadata == {"test": 1, "inlet": "10m"}
    assert (
        result.data.time.values == pd.date_range("2024-01-01", "2024-05-01", inclusive="left").values
    ).all()


def test_combine_data_objects_by_inlet(make_obs_data):
    data_objects = make_obs_data

    result = combine_and_elevate_inlet(data_objects)

    expected_values = np.concatenate(
        [x * np.ones(len(ds.data.time.values)) for x, ds in zip([10.0, 11.0, 12.0, 10.0], data_objects)]
    )

    np.testing.assert_equal(result.data.inlet.values, expected_values)

    assert result.metadata["inlet"] == "multiple"


def test_combine_data_objects_by_site():
    data_objects = [make_obs_data_object("2024-01-01", "2024-02-01", metadata={"site": x}) for x in "abcd"]

    result = combine_multisite(data_objects)

    expected_dataset = xr.concat(
        [do.data.expand_dims({"site": [x]}) for do, x in zip(data_objects, "abcd")], dim="site"
    )

    xr.testing.assert_equal(result.data, expected_dataset)


@pytest.fixture()
def surface_data():
    clear_test_stores()

    # DECC network sites
    network = "DECC"
    bsd_42_path = get_surface_datapath(filename="bsd.picarro.1minute.42m.min.dat", source_format="CRDS")

    standardise_surface(store="user", filepath=bsd_42_path, source_format="CRDS", site="bsd", network=network)

    hfd_100_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    standardise_surface(
        store="user", filepath=hfd_100_path, source_format="CRDS", site="hfd", network=network
    )

    tac_path = get_surface_datapath(filename="tac.picarro.1minute.100m.test.dat", source_format="CRDS")
    standardise_surface(store="user", filepath=tac_path, source_format="CRDS", site="tac", network=network)


def test_combine_by_site_on_real_data(surface_data):
    data_objects = search_surface(store="user", species="co2", site=["hfd", "tac", "bsd"]).retrieve_all()

    expected_dataset = xr.concat(
        [do.data.expand_dims({"site": [x]}) for do, x in zip(data_objects, ["hfd", "tac", "bsd"])], dim="site"
    )
    result = combine_multisite(data_objects)

    xr.testing.assert_equal(expected_dataset, result.data)


def test_combine_and_elevate_inlet_on_real_data(surface_data):
    """Test combining non-overlapping data for same site with different inlet values"""
    data_object = search_surface(store="user", species="co2", site="hfd").retrieve_all()

    n_times = len(data_object.data.time)
    step = n_times // 3

    # split obs data into 3 pieces, and change the inlet height
    data_objects = []
    for i in range(3):
        md = data_object.metadata.copy()
        md["inlet"] = f"1{i}m"
        ds = data_object.data.isel(time=slice(i * step, (i + 1) * step), drop=True)
        data_objects.append(ObsData(md, ds))

    result = combine_and_elevate_inlet(data_objects)

    np.testing.assert_array_equal(
        result.data.inlet.values, np.array([10.0] * step + [11.0] * step + [12.0] * step)
    )


def test_combine_and_elevate_inlet_on_real_data_overlap_raises_error(surface_data):
    """Test combining non-overlapping data for same site with different inlet values"""
    data_object = search_surface(store="user", species="co2", site="hfd").retrieve_all()

    n_times = len(data_object.data.time)
    step = n_times // 3

    # split obs data into 3 pieces, and change the inlet height
    data_objects = []
    for i in range(2):
        md = data_object.metadata.copy()
        md["inlet"] = f"1{i}m"
        ds = data_object.data.isel(time=slice(i * step, (i + 2) * step), drop=True)
        data_objects.append(ObsData(md, ds))

    with pytest.raises(ValueError):
        combine_and_elevate_inlet(data_objects, override_on_conflict=False)


def test_combine_and_elevate_inlet_on_real_data_overlap_override_on_conflict(surface_data):
    """Test combining non-overlapping data for same site with different inlet values"""
    data_object = search_surface(store="user", species="co2", site="hfd").retrieve_all()

    n_times = len(data_object.data.time)
    step = n_times // 3

    # split obs data into 3 pieces, and change the inlet height
    data_objects = []
    for i in range(2):
        md = data_object.metadata.copy()
        md["inlet"] = f"1{i}m"
        ds = data_object.data.isel(time=slice(i * step, (i + 2) * step), drop=True)
        data_objects.append(ObsData(md, ds))

    result = combine_and_elevate_inlet(data_objects)

    np.testing.assert_array_equal(
        result.data.inlet.values, np.array([10.0] * step + [10.0] * step + [11.0] * step)
    )
