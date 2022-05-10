import pytest
import numpy as np
import xarray as xr
from openghg.store import DataSchema


def test_data_schema():
    """Test DataSchema can be initialised correctly and store defaults"""
    data_vars = {"y": ("t", "x")}

    data_schema = DataSchema(data_vars = data_vars)

    assert data_schema.data_vars == data_vars
    assert data_schema.dtypes is None
    assert data_schema.dims is None 


@pytest.fixture(scope="module")
def dummy_data_1():

    time = np.array(["2018-10-01", "2018-07-02"], dtype=np.datetime64)
    lat = np.zeros(2, dtype=np.float16)
    lon = np.zeros(2, dtype=np.float32)

    shape = (len(time), len(lat), len(lon))
    values = np.zeros(shape, dtype=np.float64)

    ds = xr.Dataset({"fp": (("time", "lat", "lon"), values)},
                     coords = {"time": time, "lat": lat, "lon": lon})    

    return ds


@pytest.fixture(scope="module")
def data_schema_1():

    data_vars = {"fp": ("time", "lat", "lon")}
    dims = ["time", "lat", "lon"]
    dtypes = {"fp": np.floating,
             "lat": np.floating,
             "lon": np.floating,
             "time": np.datetime64}

    data_schema = DataSchema(data_vars=data_vars,
                             dims=dims,
                             dtypes=dtypes)
    
    return data_schema


def test_data_schema_match(data_schema_1, dummy_data_1):
    """Check DataSchema can validate matching dummy data"""

    data_schema_1.validate_data(dummy_data_1)


def test_data_schema_extra(data_schema_1, dummy_data_1):
    """Check DataSchema can validate dummy data with extra variables"""

    height = np.zeros(2)
    pressure = np.zeros(len(height))
    dummy_data_extra = dummy_data_1.assign_coords({"height": height})
    dummy_data_extra = dummy_data_extra.assign({"pressure": ("height", pressure)})

    data_schema_1.validate_data(dummy_data_extra)

def test_data_schema_empty(dummy_data_1):
    """Check empty DataSchema does nothing with dummy data"""
    # TODO: May want to change this behaviour?

    data_schema = DataSchema()

    data_schema.validate_data(dummy_data_1)



