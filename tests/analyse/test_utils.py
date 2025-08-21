import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.analyse._utils import stack_datasets
from openghg.util import cf_ureg


@pytest.fixture
def flux_co2_dummy():
    """
    Create example FluxData object with dummy data
     - Species is carbon dioxide (co2)
     - Data is 2-hourly from 2011-12-31 - 2012-01-02 (inclusive)
     - Small lat, lon (TEST_DOMAIN)
     - "flux" values are in a range from 1, ntime+1, different along the time axis.
    """
    from openghg.dataobjects import FluxData

    time = pd.date_range("2011-12-31", "2012-01-02", freq="2h")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.arange(1.0, ntime + 1, 1.0)

    expand_axes = (1, 2)
    values = np.expand_dims(values, axis=expand_axes)
    for j in expand_axes:
        values = np.repeat(values, shape[j], axis=j)

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), values)}, coords={"lat": lat, "lon": lon, "time": time}
    )

    # add units for testing with pint
    flux.flux.attrs["units"] = "mol m-2 s-1"
    flux.lat.attrs["units"] = "degrees_north"
    flux.lon.attrs["units"] = "degrees_east"

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "co2"
    metadata = {"species": species, "source": "TESTSOURCE", "domain": "TESTDOMAIN"}

    fluxdata = FluxData(data=flux, metadata=metadata)

    return fluxdata


@pytest.fixture
def flux_co2_dummy_1h():
    """
    Create example FluxData object with dummy data
     - Species is carbon dioxide (co2)
     - Data is 2-hourly from 2011-12-31 - 2012-01-02 (inclusive)
     - Small lat, lon (TEST_DOMAIN)
     - "flux" values are in a range from 1, ntime+1, different along the time axis.
    """
    from openghg.dataobjects import FluxData

    time = pd.date_range("2011-12-31", "2012-01-02", freq="1h")
    lat = [1.0, 2.0]
    lon = [10.0, 20.0]

    nlat, nlon, ntime = len(lat), len(lon), len(time)
    shape = (ntime, nlat, nlon)
    values = np.arange(1.0, ntime + 1, 1.0)

    expand_axes = (1, 2)
    values = np.expand_dims(values, axis=expand_axes)
    for j in expand_axes:
        values = np.repeat(values, shape[j], axis=j)

    flux = xr.Dataset(
        {"flux": (("time", "lat", "lon"), values)}, coords={"lat": lat, "lon": lon, "time": time}
    )

    # add units for testing with pint
    flux.flux.attrs["units"] = "mol m-2 s-1"
    flux.lat.attrs["units"] = "degrees_north"
    flux.lon.attrs["units"] = "degrees_east"

    # Potential metadata:
    # - title, author, date_creaed, prior_file_1, species, domain, source, heights, ...
    # - data_type?
    species = "co2"
    metadata = {"species": species, "source": "TESTSOURCE", "domain": "TESTDOMAIN"}

    fluxdata = FluxData(data=flux, metadata=metadata)

    return fluxdata


def test_stack_datasets(flux_co2_dummy):
    """Check that stacking the same data twice is equivalent to doubling the values."""
    fluxes = [flux_co2_dummy.data, flux_co2_dummy.data]

    stacked = stack_datasets(fluxes)

    xr.testing.assert_equal(stacked, 2 * fluxes[0])


def test_stack_datasets_different_frequencies(flux_co2_dummy, flux_co2_dummy_1h):
    """Check that stacking the same data twice is equivalent to doubling the values."""
    fluxes = [flux_co2_dummy.data, flux_co2_dummy_1h.data]

    stacked = stack_datasets(fluxes)
    stacked_values = stacked.flux.values[:, 0, 0]

    flux_1h_values = np.arange(1.0, len(fluxes[1].time) + 1, 1.0)

    # make indices to forward fill
    ffill_indices = []
    for i in range(len(fluxes[0].time)):
        ffill_indices.append(i)
        ffill_indices.append(i)

    ffill_indices = ffill_indices[:-1]  # last value is not filled

    flux_2h_values_filled = np.arange(1.0, len(fluxes[0].time) + 1, 1.0)[ffill_indices]
    expected_values = flux_1h_values + flux_2h_values_filled

    assert np.all(stacked_values == expected_values)


def test_stack_datasets_units(flux_co2_dummy):
    """Check that stacking preserves units."""
    fluxes = [flux_co2_dummy.data, flux_co2_dummy.data]
    stacked = stack_datasets(fluxes)

    assert stacked.flux.attrs.get("units") == fluxes[0].flux.attrs["units"]


def test_stack_datasets_with_different_units(flux_co2_dummy):
    """Check that stacking fluxes with different units."""
    flux_ds = flux_co2_dummy.data
    flux_ds_ppb = flux_ds.pint.quantify().pint.to("ppb mol m-2 s-1").pint.dequantify()

    fluxes = [flux_ds, flux_ds_ppb]
    stacked = stack_datasets(fluxes)

    # Check for equality after converting to expected units; we do not know ahead of time
    # which units will be used (it depends on the order of the fluxes and their units)
    # so we convert in both test cases

    # if we convert stacked to mol m-2 s-1, it should equal twice the first flux
    xr.testing.assert_allclose(stacked.pint.quantify().pint.to("mol m-2 s-1"), 2 * fluxes[0].pint.quantify())

    # if we convert stack to ppb mol m-2 s-1, it should equal twice the second flux
    xr.testing.assert_allclose(
        stacked.pint.quantify().pint.to("ppb mol m-2 s-1"), 2 * fluxes[1].pint.quantify()
    )
