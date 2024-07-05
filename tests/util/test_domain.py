import numpy as np
import pytest
import xarray as xr
from openghg.util import convert_internal_longitude, convert_lon_to_180, convert_lon_to_360, cut_data_extent, find_domain
from openghg.util._domain import _get_coord_data


def test_find_domain():
    """Test find_domain function can be used to find correct lat, lon values"""
    domain = "EUROPE"

    latitude, longitude = find_domain(domain)

    assert latitude[0] == 1.072900009155273438e01
    assert latitude[-1] == 7.905699920654296875e01
    assert longitude[0] == -9.790000152587890625e01
    assert longitude[-1] == 3.938000106811523438e01


def test_find_domain_missing():
    """Test find_domain function returns an error if domain is not present"""
    domain = "FAKE"

    with pytest.raises(ValueError) as e_info:
        find_domain(domain)


def test_create_coord():
    """Test coordinate values can be created from range and increment"""
    coord = "coordinate"
    domain = "TEST"

    coord_range = ["-10", "10"]
    increment = "0.1"
    data = {"coordinate_range": coord_range, "coordinate_increment": increment}

    coordinate = _get_coord_data(coord, data, domain)

    assert np.isclose(coordinate[0], float(coord_range[0]))
    assert np.isclose(coordinate[-1], float(coord_range[-1]))

    av_increment = (coordinate[1:] - coordinate[:-1]).mean()
    assert np.isclose(av_increment, float(increment))


@pytest.mark.parametrize(
    "lon_in,expected_lon_out",
    [
        (np.array([360.0]), np.array([0.0])),
        (np.array([181.0]), np.array([-179.0])),
        (np.array([-180.0, 0.0, 179.9]), np.array([-180.0, 0.0, 179.9])),
        (np.arange(1, 361, 1), np.concatenate([np.arange(1, 180, 1), np.arange(-180, 1, 1)])),
    ],
)
def test_convert_longitude(lon_in, expected_lon_out):
    """Test expected longitude conversion for individual values and range."""
    lon_out = convert_lon_to_180(lon_in)
    np.testing.assert_allclose(lon_out, expected_lon_out)


@pytest.mark.parametrize(
    "lon_in,expected_lon_out",
    [
        (np.array([360.0]), np.array([0.0])),
        (np.array([181.0]), np.array([181.0])),
        (np.array([-180.0, 0.0, 179.9]), np.array([180.0, 0.0, 179.9])),
        (np.arange(1, 360, 1), np.arange(1, 360, 1)),
    ],
)
def test_convert_longitude_scale(lon_in, expected_lon_out):
    lon_out = convert_lon_to_360(lon_in)
    np.testing.assert_allclose(lon_out, expected_lon_out)


@pytest.mark.parametrize(
    "lon_start,lon_stop,expected_lon_start,expected_lon_stop",
    [
        (0, 360, -180, 180),
        (-180, 180, -180, 180),
    ],
)
def test_convert_internal_longitude_ds(lon_start, lon_stop, expected_lon_start, expected_lon_stop):
    """
    Test longitude values can be converted and reordered within a Dataset
    """
    import xarray as xr

    step = 1.0

    # Create input Dataset
    input_lon = np.arange(lon_start, lon_stop, step)
    x = np.linspace(0, 10, len(input_lon))
    data = xr.Dataset({"x": ("lon", x)}, coords={"lon": input_lon})

    # Define expected outputs (reordered by default)
    expected_lon = np.arange(expected_lon_start, expected_lon_stop, step)

    start_expected_x = data["x"].sel({"lon": slice(180, 360)}).values
    end_expected_x = data["x"].sel({"lon": slice(-180, 179)}).values
    expected_x = np.concatenate([start_expected_x, end_expected_x])

    # Convert longitude for all data variables within Dataset
    data = convert_internal_longitude(data)

    np.testing.assert_allclose(data["x"].values, expected_x)
    np.testing.assert_allclose(data["lon"].values, expected_lon)


@pytest.mark.parametrize(
    "lon_start,lon_stop,expected_lon_start,expected_lon_stop",
    [
        (0, 360, -180, 180),
        (-180, 180, -180, 180),
    ],
)
def test_convert_internal_longitude_da(lon_start, lon_stop, expected_lon_start, expected_lon_stop):
    """
    Test longitude values can be converted and reordered.
    Same as above but this time with a DataArray rather than a Dataset.
    """

    step = 1.0

    # Create input DataArray
    input_lon = np.arange(lon_start, lon_stop, step)
    x = np.linspace(0, 10, len(input_lon))
    da = xr.DataArray(x, coords={"lon": input_lon}, dims=("lon",))

    # Define expected outputs (reordered by default)
    expected_lon = np.arange(expected_lon_start, expected_lon_stop, step)

    start_expected_x = da.sel({"lon": slice(180, 360)}).values
    end_expected_x = da.sel({"lon": slice(-180, 179)}).values
    expected_x = np.concatenate([start_expected_x, end_expected_x])

    # Convert longitude within DataArray
    da = convert_internal_longitude(da)

    np.testing.assert_allclose(da.values, expected_x)
    np.testing.assert_allclose(da["lon"].values, expected_lon)


def test_cut_data_extent():
    """
    Test DataArray can be cut down based on output lat, lon values.
    """
    # Defining input data
    step_lat_in = 0.1
    step_lon_in = 0.2

    lat_in = np.arange(0, 10, step_lat_in)
    lon_in = np.arange(10, 20, step_lon_in)

    shape = (len(lat_in), len(lon_in))
    da = xr.DataArray(np.zeros(shape), coords={"lat": lat_in, "lon": lon_in})

    step = 1
    lat_out = np.arange(5, 7, step)
    lon_out = np.arange(11, 19, step)

    # Applying cut function
    da_cut = cut_data_extent(da, lat_out, lon_out)

    # Creating expected output
    # - derived step from lat_out and lon_out used as a buffer
    # - slice also seems to mean top value is included if present.
    expected_lat_cut = np.arange(lat_out[0] - step, lat_out[-1] + step + step_lat_in, step_lat_in)
    expected_lon_cut = np.arange(lon_out[0] - step, lon_out[-1] + step + step_lon_in, step_lon_in)

    np.testing.assert_allclose(da_cut["lat"].values, expected_lat_cut)
    np.testing.assert_allclose(da_cut["lon"].values, expected_lon_cut)
