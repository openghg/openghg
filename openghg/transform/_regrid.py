# TODO: Add import for xesmf within specific functions and NOT general import at the top.
# TODO: Add try-except around xesmf import with helpful error message

import numpy as np
from numpy import ndarray
import xarray as xr
from typing import Union, Optional, Tuple, cast

xesmf_error_message = (
    "Unable to import xesmf for use with regridding algorithms"
    "To use transform modules please follow instructions"
    "for installing non-python dependencies (requires conda"
    "to be installed even if using pip to install other packages)"
)
# TODO: Add explicit link to instruction page once created


def _getGridCC(lat: ndarray, lon: ndarray) -> Tuple[ndarray, ndarray]:
    """
    Create a cell centered meshgrid for from 1D arrays of lon, lat
    This meshgrid defines the bounds of each cell.
    """
    # dx = lon[2]-lon[1]
    # dy = lat[2]-lat[1]
    dx = np.mean(lon[1:] - lon[:-1])
    dy = np.mean(lat[1:] - lat[:-1])
    lon = np.append(lon, lon[-1] + dx)
    lat = np.append(lat, lat[-1] + dy)
    lon -= dx / 2.0
    lat -= dy / 2.0
    LON, LAT = np.meshgrid(lon, lat)
    return LON, LAT


def _create_xesmf_grid_uniform_cc(lat: ndarray, lon: ndarray) -> xr.Dataset:
    """
    Creates a Dataset ready to be used by the xesmf regridder from 1D arrays
    of latitude and longitude values.

    This includes both the centre points and bounds of the latitude and longitude
    grids (needed for conservative regridding methods but not for bilinear etc.)
    """
    LON, LAT = np.meshgrid(lon, lat)
    LON_b, LAT_b = _getGridCC(lat, lon)

    grid = xr.Dataset(
        {
            "lon": (["x", "y"], LON),
            "lat": (["x", "y"], LAT),
            "lon_b": (["x_b", "y_b"], LON_b),
            "lat_b": (["x_b", "y_b"], LAT_b),
        }
    )

    return grid


def convert_to_ndarray(array: Union[ndarray, xr.DataArray]) -> ndarray:
    """Check and extract underlying numpy array from DataArray as necessary"""

    if isinstance(array, xr.DataArray):
        values = array.values
    else:
        values = array

    return values


def regrid_uniform_cc(
    data: Union[ndarray, xr.DataArray],
    lat_out: Union[ndarray, xr.DataArray],
    lon_out: Union[ndarray, xr.DataArray],
    lat_in: Optional[Union[ndarray, xr.DataArray]] = None,
    lon_in: Optional[Union[ndarray, xr.DataArray]] = None,
    latlon: Optional[list] = None,
    method: str = "conservative",
) -> Union[ndarray, xr.DataArray]:
    """
    Regrid data between two uniform, cell centered grids.
    All coordinates (lat_out, lon_out, lat_in, lon_in) should be for the centre
    of the representative cell and in degrees.

    Adapted from code written by @DTHoare

    Args:
        data: Data to be regridded.
            Data must have dimensions (lat, lon) if 2D or (time, lat, lon) if 3D.
        lat_out: 1D array for output latitude grid
        lon_out: 1D array for output longituide grid
        lat_in: 1D array for input latitude grid.
            Only used if data is a numpy array and not a DataArray
        lon_in: 1D array for input longitude grid.
            Only used if data is a numpy array and not a DataArray
        latlon: Names for latitude and longitude coordinates within data.
            Default = ["lat", "lon"]
        method: Method to use for regridding. Mainly use:
            - "conservative"
            - "conservative_normed" (ignores NaN values)
            See xesmf documentation for full list of options.

    Returns:
        ndarray / DataArray : Regridded data using specified method
    """
    try:
        import xesmf  # type:ignore
    except ImportError:
        raise ImportError(xesmf_error_message)

    if latlon is None:
        latlon = ["lat", "lon"]

    if isinstance(data, xr.DataArray):
        lat_in_extracted = data[latlon[0]].values
        lon_in_extracted = data[latlon[1]].values

        if lat_in is not None:
            if not np.isclose(lat_in, lat_in_extracted):
                raise ValueError(
                    "Input for 'lat_in' should not be supplied if 'data' is a DataArray object.\n"
                    "Please check 'lat_out' have been supplied correctly as well."
                )

        if lon_in is not None:
            if not np.isclose(lon_in, lon_in_extracted):
                raise ValueError(
                    "Input for 'lon_in' should not be supplied if 'data' is a DataArray object.\n"
                    "Please check 'lon_out' has been supplied correctly as well."
                )

        lat_in = lat_in_extracted
        lon_in = lon_in_extracted

    lat_in = cast(Union[ndarray, xr.DataArray], lat_in)
    lon_in = cast(Union[ndarray, xr.DataArray], lon_in)

    if data.shape != (lat_in.size, lon_in.size):
        raise ValueError(
            f"Shape of input 'data' {data.shape}"
            f"does not match 'lat_in' and 'lon_in' lengths: {len(lat_in)}, {len(lon_in)}"
        )

    lat_in = convert_to_ndarray(lat_in)
    lon_in = convert_to_ndarray(lon_in)
    lat_out = convert_to_ndarray(lat_out)
    lon_out = convert_to_ndarray(lon_out)

    input_grid = _create_xesmf_grid_uniform_cc(lat_in, lon_in)
    output_grid = _create_xesmf_grid_uniform_cc(lat_out, lon_out)

    regridder = xesmf.Regridder(input_grid, output_grid, method)
    regridded: Union[ndarray, xr.DataArray] = regridder(data)

    if isinstance(regridded, xr.DataArray):
        from scipy.sparse import coo_matrix  # type:ignore

        # Checking dimensions and mapping back lat_out and lon_out
        # May be overkill but attempting to make sure this is done correctly.

        # The regridded DataArray will contain dimensions of ('x', 'y') for
        # data, 'lat' and 'lon' coordinates e.g. lon = [[-10,-10],[0,0]]
        # This is to allow for the generic case where ('lat', 'lon') is not
        # a rectilinear (uniform) grid.
        # Since we are creating a uniform grid we want to flatten this and
        # put data back on ('lat', 'lon') dimension.
        regridded_stack = regridded.stack(z=("x", "y"))
        lat_coord = regridded_stack["lat"]
        lon_coord = regridded_stack["lon"]

        # Find coords within our lat_out and lon_out grid for our regridded output
        # Digitize is technically a binning operation outputting the number of the
        # bin the value is within so to get indicies we can subtract 1.
        lat_index = np.digitize(lat_coord, lat_out) - 1
        lon_index = np.digitize(lon_coord, lon_out) - 1

        # Create a sparse matrix from the flattened data and the lat, lon index values.
        # Reshape to our required lat_out, lon_out and output as a numpy array
        shape = (len(lat_out), len(lon_out))
        regridded_grid = coo_matrix((regridded_stack.data, (lat_index, lon_index)), shape=shape).toarray()

        regridded = xr.DataArray(
            regridded_grid,
            dims=("lat", "lon"),
            coords={"lat": lat_out, "lon": lon_out},
            attrs=regridded.attrs,
        )

        # # Alternative, more simplistic approach. May not be generic
        # regridded = regridded.drop(labels=("lat","lon"))
        # regridded = regridded.assign_coords(**{"x":output_lat,"y":output_lon})
        # regridded = regridded.rename({"x":"lat","y":"lon"})

    regridder.clean_weight_file()

    return regridded


# def regrid_betweenGrids(data, input_grid, output_grid, method="conservative"):
#     """
#     Regrid data from predefined input_grid and output_grid

#     Inputs
#         data - numpy array

#         input_grid, output_grid -  Dataset of the form:

#             xr.Dataset({'lat': (['x', 'y'], LAT),
#                          'lon': (['x', 'y'], LON),
#                          'lat_b': (['x_b', 'y_b'], LAT_b),
#                          'lon_b': (['x_b', 'y_b'], LON_b)})

#             where lat and lon give cell centre locations, and lat_b and lon_b give the cell bounds (corners)

#         method - string describing method to use.
#             Should be one of "conservative" or "conservative_normed".
#             Note that to use the conservative_normed method you need to have installed the "masking" development
#             branch of the xesmf package (contact Daniel for more details).

#             With the 'masking' branch of xESMF you can include a mask in the input_grid to ignore nan values
#     returns
#         regridded numpy array
#     """
#     try:
#         import xesmf
#     except ImportError:
#         raise ImportError(xesmf_error_message)

#     #if 'mask' in input_grid:
#     #    # !!! Requires the 'masking' branch of xESMF to be manually installed !!!
#     #    method = 'conservative_normed'
#     #else:
#     #method = 'conservative'
#     regridder = xesmf.Regridder(input_grid, output_grid, method)
#     regridded = regridder( data )
#     regridder.clean_weight_file()

#     return regridded
