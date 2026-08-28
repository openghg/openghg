import numpy as np
import pytest
import xarray as xr
from openghg.store import DataSchema
from openghg.types import ValidationError
from openghg.util import cf_ureg


def test_data_schema():
    """Test DataSchema can be initialised correctly and store defaults"""
    data_vars = {"y": ("t", "x")}

    data_schema = DataSchema(data_vars=data_vars)

    assert data_schema.data_vars == data_vars
    assert data_schema.dtypes is None
    assert data_schema.dims is None


@pytest.fixture(scope="module")
def dummy_data_1():
    time = np.array(["2018-10-01", "2018-07-02"], dtype=np.datetime64)
    lat = np.zeros(2, dtype=np.float32)
    lon = np.zeros(2, dtype=np.float32)

    shape = (len(time), len(lat), len(lon))
    values = np.zeros(shape, dtype=np.float64)

    ds = xr.Dataset({"fp": (("time", "lat", "lon"), values)}, coords={"time": time, "lat": lat, "lon": lon})

    return ds


@pytest.fixture(scope="module")
def data_schema_1():
    data_vars = {"fp": ("time", "lat", "lon")}
    dims = ["time", "lat", "lon"]
    dtypes = {"fp": np.floating, "lat": np.floating, "lon": np.floating, "time": np.datetime64}

    data_schema = DataSchema(data_vars=data_vars, dims=dims, dtypes=dtypes)

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


def test_data_schema_missing_variable(data_schema_1, dummy_data_1):
    with pytest.raises(ValidationError, match="fp"):
        data_schema_1.validate_data(dummy_data_1.drop_vars("fp"))


def test_data_schema_wrong_dtype(data_schema_1, dummy_data_1):
    with pytest.raises(ValidationError, match="dtype mismatch"):
        data_schema_1.validate_data(dummy_data_1.assign(fp=dummy_data_1.fp.astype(int)))


def test_data_schema_missing_variable_dimension(data_schema_1, dummy_data_1):
    with pytest.raises(ValueError, match="Missing dimension for data variable: fp, lon"):
        data_schema_1.validate_data(dummy_data_1.rename(lon="height"))


def test_data_schema_requires_units_on_named_variables_and_coordinates():
    data = xr.Dataset(
        {
            "flux": (
                ("time", "lat", "lon"),
                np.ones((1, 1, 1)),
                {"units": "umol m-2 s-1"},
            )
        },
        coords={
            "time": np.array(["2020-01-01"], dtype="datetime64[ns]"),
            "lat": ("lat", [51.0], {"units": "degrees_north"}),
            "lon": ("lon", [-2.0], {"units": "degrees_east"}),
        },
    )
    schema = DataSchema(
        data_vars={"flux": ("time", "lat", "lon")},
        units={"lat": "degrees_north", "lon": "degrees_east"},
        units_compatible={"flux": "mol m-2 s-1"},
    )

    schema.validate_data(data)
    quantified = data.pint.quantify(unit_registry=cf_ureg)
    assert quantified.flux.pint.units == cf_ureg("umol m-2 s-1").units

    missing_units = data.copy()
    del missing_units["flux"].attrs["units"]
    with pytest.raises(ValidationError, match="units"):
        schema.validate_data(missing_units)

    with pytest.raises(ValidationError, match="not compatible"):
        schema.validate_data(data.assign(flux=data.flux.assign_attrs(units="ppm")))


def test_data_schema_requires_named_variable_and_dataset_attributes():
    data = xr.Dataset(
        {"ch4": ("time", [1900.0], {"long_name": "methane_mole_fraction"})},
        coords={"time": np.array(["2020-01-01"], dtype="datetime64[ns]")},
        attrs={"species": "ch4"},
    )
    schema = DataSchema(
        data_vars={"ch4": ("time",)},
        required_attrs={"ch4": {"long_name"}},
        dataset_attrs={"species"},
    )

    schema.validate_data(data)

    missing_variable_attr = data.copy()
    del missing_variable_attr.ch4.attrs["long_name"]
    with pytest.raises(ValidationError, match="long_name"):
        schema.validate_data(missing_variable_attr)

    missing_dataset_attr = data.copy()
    del missing_dataset_attr.attrs["species"]
    with pytest.raises(ValidationError, match="species"):
        schema.validate_data(missing_dataset_attr)
