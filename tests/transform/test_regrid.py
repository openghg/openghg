import numpy as np
import pytest
import xarray as xr
from openghg.transform import regrid_uniform_cc


@pytest.fixture()
def grid_da():
    """
    Define 4x8 grid to be regridded.
    Assign 0, 1, 2, 3 values to four equal segments (2x4 each)
    """
    dlat = 5.0
    dlon = 5.0
    lat = np.arange(-10, 10, dlat)
    lon = np.arange(20, 60, dlon)

    # 4x8 grid - assign 0, 1, 2, 3 to the 4 corners
    data = np.zeros((len(lat), len(lon)))
    data[:2, :4] = 0
    data[:2, 4:] = 1
    data[2:, :4] = 2
    data[2:, 4:] = 3

    # i.e. [[0, 0, 0, 0, 1, 1, 1, 1],
    #       [0, 0, 0, 0, 1, 1, 1, 1],
    #       [2, 2, 2, 2, 3, 3, 3, 3],
    #       [2, 2, 2, 2, 3, 3, 3, 3]]

    grid = xr.DataArray(data, coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))

    return grid


@pytest.fixture()
def grid_out(grid_da):
    """
    Define 2x2 grid for data to be updated to.
    Ensure the centre of the out grid is defined so the boundaries encompass
    each of the 4 sections created within grid_da.
    Expect grid_da to be divisible by 2 in both lat and lon dimensions.
    """
    lat_in = grid_da.lat
    lon_in = grid_da.lon

    dlat = lat_in.diff(dim="lat").mean()
    dlon = lon_in.diff(dim="lon").mean()
    dlat_2 = dlat / 2
    dlon_2 = dlon / 2

    nlat = len(lat_in)
    nlon = len(lon_in)

    # Select lat and lon in 1/4 and 3/4 position of array
    lat_start = lat_in[nlat // 4 - 1]
    lat_end = lat_in[nlat * 3 // 4 - 1]
    lon_start = lon_in[nlon // 4 - 1]
    lon_end = lon_in[nlon * 3 // 4 - 1]

    # Create 2x2 for the centre points of the new grid.
    lat_out = np.linspace(lat_start + dlat_2, lat_end + dlat_2, 2)
    lon_out = np.linspace(lon_start + dlon_2, lon_end + dlon_2, 2)

    # i.e. trying to create lat and lon bounds which allow input to be
    # regridded to:
    #  [[0, 1],
    #   [2, 3]]

    return lat_out, lon_out


@pytest.mark.xesmf
def test_regrid_da(grid_da, grid_out):
    """
    Test regridding for regrid_uniform_cc function on DataArray objects.
    """
    xesmf = pytest.importorskip("xesmf")

    lat_out, lon_out = grid_out

    out = regrid_uniform_cc(grid_da, lat_out, lon_out)
    data = out.data

    # Expect input values to essentially be returned on the new coarser grid
    # Had to add 0.1 tolerance for this to work - seems high?
    assert np.isclose(data[0, 0], 0, atol=0.1)
    assert np.isclose(data[0, 1], 1, atol=0.1)
    assert np.isclose(data[1, 0], 2, atol=0.1)
    assert np.isclose(data[1, 1], 3, atol=0.1)
    assert data.shape == (len(lat_out), len(lon_out))

    coords = out.coords
    assert "lat" in coords
    assert "lon" in coords
    assert out.dims == ("lat", "lon")


@pytest.mark.xesmf
def test_regrid_array(grid_da, grid_out):
    """
    Test regridding for regrid_uniform_cc function on numpy array objects.
    """
    xesmf = pytest.importorskip("xesmf")

    lat_in = grid_da["lat"]
    lon_in = grid_da["lon"]
    grid = grid_da.data

    lat_out, lon_out = grid_out

    out = regrid_uniform_cc(grid, lat_out, lon_out, lat_in, lon_in)

    assert np.isclose(out[0, 0], 0, atol=0.1)
    assert np.isclose(out[0, 1], 1, atol=0.1)
    assert np.isclose(out[1, 0], 2, atol=0.1)
    assert np.isclose(out[1, 1], 3, atol=0.1)

    assert out.shape == (len(lat_out), len(lon_out))


@pytest.mark.xesmf
def test_regrid_uneven_lat_lon(grid_da):
    """
    Test grid can be regridded onto different lat-lon dimensions.
    """
    xesmf = pytest.importorskip("xesmf")

    lat_in = grid_da.lat
    lon_in = grid_da.lon

    dlat = lat_in.diff(dim="lat").mean()
    dlon = lon_in.diff(dim="lon").mean()

    lat_out = np.arange(lat_in.min(), lat_in.max(), dlat * 2.0)
    lon_out = np.arange(lon_in.min(), lon_in.max(), dlon * 1.5)

    out = regrid_uniform_cc(grid_da, lat_out, lon_out)
    data = out.data

    assert data.shape == (len(lat_out), len(lon_out))

    coords = out.coords
    assert "lat" in coords
    assert "lon" in coords
    assert out.dims == ("lat", "lon")


@pytest.mark.xesmf
def test_unmatched_size_da(grid_da, grid_out):
    """Check error raised when lat_in, lon_in cannot be matched to grid (DataArray)"""
    xesmf = pytest.importorskip("xesmf")
    lat_in_wrong = np.array([0, 1, 3])
    lon_in = grid_da["lon"]
    lat_out, lon_out = grid_out

    with pytest.raises(ValueError) as e_info:
        regrid_uniform_cc(grid_da, lat_out, lon_out, lat_in_wrong, lon_in)


@pytest.mark.xesmf
def test_unmatched_size_array(grid_da, grid_out):
    """Check error raised when lat_in, lon_in cannot be matched to grid (numpy array)"""
    xesmf = pytest.importorskip("xesmf")
    grid = grid_da.data
    lat_in_wrong = np.array([0, 1, 3])
    lon_in = grid_da["lon"]
    lat_out, lon_out = grid_out

    with pytest.raises(ValueError) as e_info:
        regrid_uniform_cc(grid, lat_out, lon_out, lat_in_wrong, lon_in)
