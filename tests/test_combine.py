import re
from typing import Optional

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.dataobjects import ObsData
from openghg.combine import combine_data_objects


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

    return obs_dat1, obs_dat2, obs_dat3, obs_dat4


def test_combine_data_objects(make_obs_data):
    data_objects = list(make_obs_data)
    result = combine_data_objects(data_objects)

    assert result.metadata == {"test": 1, "inlet": "10m"}
    assert (result.data.time.values == pd.date_range("2024-01-01", "2024-05-01", inclusive="left").values).all()


def test_combine_data_objects_by_inlet(make_obs_data):
    data_objects = list(make_obs_data)


    def preprocess(x: ObsData) -> ObsData:
        inlet_pat = re.compile(r"\d+(\.)?\d*")  # find decimal number

        m = inlet_pat.search(x.metadata.get("inlet", ""))
        if m:
            try:
                inlet_height = float(m.group(0))
            except TypeError:
                inlet_height = np.nan
        else:
            inlet_height = np.nan

        new_da = (inlet_height * xr.ones_like(x.data.mf)).rename("inlet")
        new_ds = xr.merge([x.data, new_da])

        new_metadata = {k: v for k, v in x.metadata.items() if k != "inlet"}

        return ObsData(new_metadata, new_ds)

    result = combine_data_objects(data_objects, preprocess=preprocess)

    expected_values = np.concatenate([x * np.ones(len(ds.data.time.values)) for x, ds in zip([10.0, 11.0, 12.0, 10.0], data_objects)])

    np.testing.assert_equal(result.data.inlet.values, expected_values)

    assert "inlet" not in result.metadata


def test_combine_data_objects_by_site():
    data_objects = [make_obs_data_object("2024-01-01", "2024-02-01", metadata={"site": x}) for x in "abcd"]


    def preprocess(x: ObsData) -> ObsData:
        new_ds = x.data.expand_dims({"site": [x.metadata["site"]]})
        new_metadata = {}
        return ObsData(new_metadata, new_ds)

    result = combine_data_objects(data_objects, preprocess=preprocess)

    expected_dataset = xr.concat([do.data.expand_dims({"site": [x]}) for do, x in zip(data_objects, "abcd")], dim="site")

    xr.testing.assert_equal(result.data, expected_dataset)
