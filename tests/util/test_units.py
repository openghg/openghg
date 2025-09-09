import pytest
import xarray as xr

import openghg


def test_pint_anywhere():
    """Check that importing openghg activates custom pint units."""
    # make a data array with 1e-9 as units
    da = xr.DataArray(list(range(10)))
    da.attrs["units"] = "1e-9"

    # the following shouldn't raise an error
    da = da.pint.quantify()

    # check that units have been converted;
    # we're printing with "cf" format since the default is to
    # convert back to 1e-9
    assert f"{da.pint.units:cf}" == "parts_per_billion"


# NOTE: doing import here because previous test needs to check that just importing openghg
# is sufficient to activate our unit registry (although maybe the test fixtures/conftest will
# have already caused this to happen...)
from openghg.util import cf_ureg


@pytest.mark.parametrize(
    "format, expected_units",
    [
        (None, "1e-9"),  # cf_ureg's default format, which is "openghg"
        ("openghg", "1e-9"),
        ("cf", "parts_per_billion"),  # cf format defined by cf_xarray
        ("D", "ppb"),  # pint's default format,
    ],
)
def test_formatting(format, expected_units):
    """Test that we can specify the format of units when a DataArray is dequantified."""
    da = xr.DataArray(list(range(10)))
    da.attrs["units"] = "1e-9"
    da = da.pint.quantify()

    # normally you would use e.g. f"{da.pint.units:cf}" to get "cf" style formatting,
    # but we have the format as a string, so we can't do this in a nice way
    assert da.pint.units.__format__(format) == expected_units


def test_hecto_pascal():
    """Check our alias hpa for hectopascal."""
    pressure_hpa = 1013.25 * cf_ureg.hpa
    pressure_pa = 101325.0 * cf_ureg.Pa

    assert pressure_pa == pressure_hpa.to("Pa")


@pytest.mark.parametrize("prefix", ["per", "per ", "per_"])
@pytest.mark.parametrize("suffix", ["mille", "mil", "meg"])
def test_per_mille_per_meg(prefix, suffix):
    """Test aliases for permille and permeg."""
    expected = "permille" if "mil" in suffix else "permeg"

    given = prefix + suffix

    converted = cf_ureg.parse_units(given)

    assert converted == expected


@pytest.mark.parametrize(
    "number,abbrev,long",
    [
        ("1e-6", "ppm", "ppm"),  # ppm is built-in to pint, so we might be stuck with ppm
        ("1e-9", "ppb", "parts_per_billion"),
        ("1e-12", "ppt", "parts_per_trillion"),
        ("1e-15", "ppq", "parts_per_quadrillion"),
    ],
)
def test_parts_per(number, abbrev, long):
    converted = cf_ureg.parse_units(number)

    # default pint formatting will conserve the pint unit
    assert abbrev == f"{converted:D}"

    # "cf" formatting will use long name
    assert long == f"{converted:cf}"
