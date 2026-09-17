import numpy as np
import pytest
import xarray as xr
from openghg.store import DataSchema, Footprints, ObsSurface
from openghg.util import cf_ureg  # noqa: F401  # Registers the xarray Pint accessor.


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


def test_data_schema_assigns_and_ignores_units():
    """Schemas should add physical units and remove units for ignored variables."""
    data = xr.Dataset(
        {
            "length": ("time", [1.0, 2.0]),
            "flag": ("time", [True, False], {"units": "not-a-pint-unit"}),
        }
    )
    data_schema = DataSchema(units={"length": "m", "flag": None})

    data_schema.validate_data(data)

    assert data.length.attrs["units"] == "m"
    assert "units" not in data.flag.attrs
    assert str(data.pint.quantify().length.pint.units) == "m"


def test_pint_quantify_allows_unitless_data_variables():
    """Unitless counters and binary values may be left outside Pint."""
    data = xr.Dataset(
        {
            "number_of_observations": ("time", [1, 2]),
            "status_flag": ("time", [True, False]),
        }
    )

    quantified = data.pint.quantify()

    assert quantified.number_of_observations.pint.units is None
    assert quantified.status_flag.pint.units is None


def test_footprint_schema_assigns_units_and_ignores_particle_ages():
    """Footprint defaults quantify model terms but not age values used numerically."""
    data = xr.Dataset(
        {
            "fp": (("time", "lat", "lon"), np.ones((1, 1, 1))),
            "particle_locations_n": (("time", "lon", "height"), np.ones((1, 1, 1))),
            "mean_age_particles_n": (
                ("time", "lon", "height"),
                np.ones((1, 1, 1)),
                {"units": "not-a-pint-unit"},
            ),
        },
        coords={
            "time": np.array(["2020-01-01"], dtype="datetime64[ns]"),
            "lat": [51.0],
            "lon": [-2.0],
            "height": [100.0],
        },
    )

    Footprints.schema(particle_locations=False).assign_units(data)

    assert data.fp.attrs["units"] == "m2 s mol-1"
    assert data.particle_locations_n.attrs["units"] == "1"
    assert data.lat.attrs["units"] == "degrees_north"
    assert data.lon.attrs["units"] == "degrees_east"
    assert "units" not in data.mean_age_particles_n.attrs


def test_surface_schema_assigns_species_default_unit():
    """Species schema defaults retain the internal mole-fraction scale."""
    data = xr.Dataset(
        {"ch4": ("time", [1800.0])},
        coords={"time": np.array(["2020-01-01"], dtype="datetime64[ns]")},
    )

    ObsSurface.schema("ch4").validate_data(data)

    assert data.ch4.attrs["units"] == "1e-9"
