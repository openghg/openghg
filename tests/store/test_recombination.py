from collections import namedtuple
from typing import Optional

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from zarr.storage import MemoryStore

from openghg.dataobjects import ObsData
from openghg.store._recombination import open_multiple_data_objects
from openghg.store.storage import LocalZarrStore


class FakeLocalZarrStore:
    def __init__(self, stores_dict: dict):
        self._stores = stores_dict


def make_mem_store(ds: xr.Dataset) -> MemoryStore:
    ms = MemoryStore()
    ds.to_zarr(store=ms)
    return ms


def make_obs_dataset(start: str, end: str) -> xr.Dataset:
    times = pd.date_range(start, end, inclusive="left")  # daily frequency
    values = np.arange(0, len(times))

    return xr.Dataset({"mf": (["time"], values)}, coords={"time": times})


def test_open_mfdataset_zarr_from_memory_store():
    """Just check that our basic set-up works"""
    ds1 = make_obs_dataset("2024-01-01", "2024-02-01")
    ds2 = make_obs_dataset("2024-02-01", "2024-03-01")

    ms1 = make_mem_store(ds1)
    ms2 = make_mem_store(ds2)

    ds3 = xr.open_mfdataset([ms1, ms2], engine="zarr")

    assert (ds3.time.values == pd.date_range("2024-01-01", "2024-03-01", inclusive="left").values).all()


def make_obs_data_object(start: str, end: str, metadata: Optional[dict] = None) -> ObsData:
    data = make_obs_dataset(start, end)
    mem_store = make_mem_store(data)

    if metadata is None:
        metadata = {}

    obs_dat = ObsData(metadata=metadata, data=data)

    # make a named tuple with a ._stores attribute to fake a LocalZarrStore
    fake_store = FakeLocalZarrStore({"v1": mem_store})
    obs_dat._zarrstore = fake_store
    obs_dat._version = "v1"

    return obs_dat


def test_open_multiple_data_objects():
    obs_dat1 = make_obs_data_object("2024-01-01", "2024-02-01", metadata={"test": 1})
    obs_dat2 = make_obs_data_object("2024-02-01", "2024-03-01", metadata={"test": 2})

    result = open_multiple_data_objects([obs_dat1, obs_dat2])

    assert result.metadata == {"test": 1}
    assert (result.data.time.values == pd.date_range("2024-01-01", "2024-03-01", inclusive="left").values).all()
