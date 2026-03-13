import numpy as np
import pytest
import xarray as xr
from helpers import get_flux_datapath

from openghg.transform.flux import parse_edgar
from openghg.transform.flux._edgar import _extract_file_info

# TODO: Add tests
# - single sector EDGAR data can be read and intepreted correctly for v6.0
#   - only monthly grid maps available?


@pytest.mark.parametrize(
    "folder,version,species,mean_raw_flux",
    [
        ("v50", "v50", "ch4", 2.261304e-11),
        ("v6.0_CH4", "v6.0", "ch4", 2.0385315e-11),
        ("v6.0_N2O", "v6.0", "n2o", 3.8895116e-13),
        ("v6.0_CO2_excl_shortcycle", "v6.0", "co2", 1.1799942e-09),
        ("v6.0_CO2_org_shortcycle", "v6.0", "co2", 2.4354319e-10),
        ("TOTALS_nc.zip", "v6.0", "ch4", 2.0385315e-11),
    ],
)
def test_parse_edgar_raw(folder, version, species, mean_raw_flux):
    """
    Test parse edgar function against different global, annual database versions.
    """
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    year = "2015"

    entry = parse_edgar(filepath, date=year, species=species)

    data_values = list(entry.values())[0]
    data = data_values["data"]

    # Raw flux from EDGAR file is in kg/m2/s - new units are mol/m2/s
    if species == "ch4":
        mol_mass = 16.0426  # g/mol
    elif species == "n2o":
        mol_mass = 44.0129  # g/mol
    elif species == "co2":
        mol_mass = 44.01  # g/mol
    mean_converted_flux = mean_raw_flux * 1e3 / mol_mass

    variable = "flux"
    assert variable in data
    assert np.isclose(data[variable].mean(), mean_converted_flux)
    assert data[variable].attrs["units"] == "mol/m2/s"

    # attrs = data.attrs

    default_domain = "globaledgar"
    # Data used here is cut down so when org domain longitude from 0.05 - 359.95001
    # is shifted onto -180 - +180 grid, this ends up as -174.85857 - +180.0
    # Note: this is *not* the same range as if the full data was used

    if species == "co2":
        # May decide to change this to split into multiple keys
        source_start = "_".join(folder.split("_")[-2:])
        source_start = source_start.replace("shortcycle", "short-cycle")
        source = f"{source_start}_anthro"
    else:
        source = "anthro"

    expected_metadata = {
        "species": species,
        "domain": default_domain,
        "source": source,
        "database": "EDGAR",
        "database_version": version,
        "author": "OpenGHG Cloud",
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        # "min_longitude": -174.85857,
        # "max_longitude": 180.0,
        "min_longitude": -180.0,
        "max_longitude": 174.85858,
        "min_latitude": -89.95,
        "max_latitude": 89.95,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    metadata = data_values["metadata"]
    assert metadata.items() >= expected_metadata.items()


@pytest.mark.xesmf
def test_parse_edgar_domain():
    """
    Test EDGAR output can be created for a pre-existing domain.
    """
    # Regridding to a new domain will use the xesmf importer - so skip this test
    # if module is not present.
    xesmf = pytest.importorskip("xesmf")

    folder = "v6.0_CH4"
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    species = "ch4"
    year = "2015"
    domain = "EUROPE"

    entry = parse_edgar(filepath, date=year, species=species, domain=domain)

    data_values = list(entry.values())[0]
    data = data_values["data"]

    variable = "flux"
    assert variable in data
    assert data[variable].attrs["units"] == "mol/m2/s"
    assert data.attrs["domain"] == domain

    from openghg.util import find_domain

    domain_lat, domain_lon = find_domain(domain)[:2]

    np.testing.assert_array_equal(data["lat"].values, domain_lat)
    np.testing.assert_array_equal(data["lon"].values, domain_lon)

    version = folder.split("_")[0]

    expected_metadata = {
        "species": species,
        "domain": domain,
        "source": "anthro",
        "database": "EDGAR",
        "database_version": version,
        "author": "OpenGHG Cloud",
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        "min_longitude": -97.9,
        "max_longitude": 39.38,
        "min_latitude": 10.729,
        "max_latitude": 79.057,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    metadata = data_values["metadata"]
    assert metadata.items() >= expected_metadata.items()


@pytest.mark.xesmf
def test_parse_edgar_new_domain():
    """
    Test EDGAR can be resampled to new, specified domain.
    """
    xesmf = pytest.importorskip("xesmf")

    folder = "v6.0_CH4"
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    species = "ch4"
    year = "2015"
    domain = "NEWDOMAIN"
    lat_out = np.arange(-10, 10, 1.0)
    lon_out = np.arange(20, 30, 1.0)

    entry = parse_edgar(filepath, date=year, species=species, domain=domain, lat_out=lat_out, lon_out=lon_out)

    data_values = list(entry.values())[0]
    data = data_values["data"]

    variable = "flux"
    assert variable in data
    assert data[variable].attrs["units"] == "mol/m2/s"
    assert data.attrs["domain"] == domain

    np.testing.assert_array_equal(data["lat"].values, lat_out)
    np.testing.assert_array_equal(data["lon"].values, lon_out)

    expected_metadata = {
        "domain": domain,
        "min_longitude": round(lon_out[0], 5),
        "max_longitude": round(lon_out[-1], 5),
        "min_latitude": round(lat_out[0], 5),
        "max_latitude": round(lat_out[-1], 5),
    }

    metadata = data_values["metadata"]
    assert metadata.items() >= expected_metadata.items()


def test_parse_edgar_unknown_domain():
    """
    Test error raised when unknown domain used and no lat, lon values provided
    """
    folder = "v6.0_CH4"
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    species = "ch4"
    year = "2015"
    domain = "FAKE"

    with pytest.raises(ValueError):
        parse_edgar(filepath, date=year, species=species, domain=domain)


@pytest.fixture(scope="session")
def edgar_v8_data():
    """
    Fixture to process the v8.0 file and returns dictionary
    """
    folder = "v8.0_CH4"
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    data = parse_edgar(datapath=filepath, date="1970")
    return data


def test_edgar_v8(edgar_v8_data):
    """
    Test to check processed data values for parse_edgar
    """
    data_values = list(edgar_v8_data.values())[0]
    assert "ch4_anthro_globaledgar_1970" in edgar_v8_data
    assert "ch4" in data_values["data"].attrs["species"]
    assert "globaledgar" in data_values["data"].attrs["domain"]
    assert "flux" in data_values["data"]


def test_parse_edgar_no_domain():
    """
    Test error raised when new lat, lon values provided but no domain name
    """
    folder = "v6.0_CH4"
    filepath = get_flux_datapath(f"EDGAR/yearly/{folder}")

    species = "ch4"
    year = "2015"
    lat_out = np.arange(-10, 10, 1.0)
    lon_out = np.arange(20, 30, 1.0)

    with pytest.raises(ValueError):
        parse_edgar(filepath, date=year, species=species, lat_out=lat_out, lon_out=lon_out)


@pytest.mark.parametrize(
    "edgar_file,expected_file_info",
    [
        (
            "v6.0_CH4_2015_TOTALS.0.1x0.1.nc",
            {"version": "v6.0", "species": "CH4", "year": 2015, "source": "TOTALS", "resolution": "0.1x0.1"},
        ),
        (
            "v50_CH4_2015.5.3x5.1.nc",
            {"version": "v50", "species": "CH4", "year": 2015, "source": "", "resolution": "5.3x5.1"},
        ),
        (
            "v432_CH4_2010_9_IPCC_6A_6D.0.1x0.1.nc",
            {
                "version": "v432",
                "species": "CH4",
                "year": 2010,
                "month": 9,
                "source": "IPCC-6A-6D",
                "resolution": "0.1x0.1",
            },
        ),
        (
            "v6.0_CO2_excl_short-cycle_org_C_2000_TOTALS.0.1x0.1.nc",
            {
                "version": "v6.0",
                "species": "CO2",
                "year": 2000,
                "source": "excl_short-cycle_TOTALS",
                "resolution": "0.1x0.1",
            },
        ),
        (
            "v6.0_CO2_org_short-cycle_C_1970_TOTALS.0.1x0.1.nc",
            {
                "version": "v6.0",
                "species": "CO2",
                "year": 1970,
                "source": "org_short-cycle_TOTALS",
                "resolution": "0.1x0.1",
            },
        ),
        (
            "v8.0_FT2022_GHG_CH4_1970_TOTALS_flx.nc",
            {"version": "v8.0", "species": "CH4", "year": 1970, "source": "TOTALS", "resolution": None},
        ),
        (
            "v8.0_FT2022_GHG_CO2_2020_TOTALS_flx_nc.zip",
            {"version": "v8.0", "species": "CO2", "year": 2020, "source": "TOTALS", "resolution": None},
        ),
        (
            "v7.0_FT2021_CO2_excl_short-cycle_org_C_2020_TOTALS.0.1x0.1.zip",
            {
                "version": "v7.0",
                "species": "CO2",
                "year": 2020,
                "source": "excl_short-cycle_TOTALS",
                "resolution": "0.1x0.1",
            },
        ),
        (
            "v7.0_FT2021_N2O_2021_FFF.zip",
            {"version": "v7.0", "species": "N2O", "year": 2021, "source": "FFF", "resolution": None},
        ),
        (
            "v7.0_FT2021_N2O_2021_TNR_Aviation_CDS.0.1x0.1.zip",
            {
                "version": "v7.0",
                "species": "N2O",
                "year": 2021,
                "source": "TNR-Aviation-CDS",
                "resolution": "0.1x0.1",
            },
        ),
        (
            "v8.0_FT2022_GHG_HFC-43-10-mee_1994_TOTALS_flx.nc",
            {
                "version": "v8.0",
                "species": "HFC-43-10-mee",
                "year": 1994,
                "source": "TOTALS",
                "resolution": None,
            },
        ),
        (
            "EDGAR_2024_GHG_CH4_2001_AGRICULTURE_flx.nc",
            {"species": "CH4", "year": 2001, "source": "AGRICULTURE"},
        ),
    ],
)
def test_extract_file_info(edgar_file, expected_file_info):
    """Test that the expected file information can be extracted from the EDGAR filename."""
    file_info = _extract_file_info(edgar_file)
    assert file_info == expected_file_info


@pytest.fixture(scope="module")
def edgar_v8_monthly_dir(tmp_path_factory):
    """
    Create a temporary directory with a synthetic monthly EDGAR v8 (2024) sectoral file.

    The file structure matches a real EDGAR v8 monthly sectoral file:
        EDGAR_2024_GHG_CH4_2001_AGRICULTURE_flx.nc

    Real file dimensions (from ncdump):
        lat = 1800 (-89.95 to 89.95, 0.1 degree increment)
        lon = 3600 (-179.95 to 179.95, 0.1 degree increment)
        time = 12 (monthly, days since 2001-01-01 00:00:00)

    A small subset of lat/lon is used for efficiency.
    """
    tmpdir = tmp_path_factory.mktemp("edgar_monthly")
    edgar_dir = tmpdir / "monthly_sectoral" / "AGRICULTURE"
    edgar_dir.mkdir(parents=True)

    # Small subset of the full globaledgar lat/lon grid (0.1 degree resolution)
    lat = np.round(np.arange(-89.95, 90.0, 0.1)[:10], 2)
    lon = np.round(np.arange(-179.95, 180.0, 0.1)[:10], 2)

    # Time values from real EDGAR 2024 file: "days since 2001-01-01 00:00:00"
    # These correspond to the 15th of each month (approximately mid-month)
    time_days = np.array([14, 45, 73, 104, 134, 165, 195, 226, 257, 287, 318, 348], dtype=np.float32)
    base_date = np.datetime64("2001-01-01")
    time_dates = base_date + time_days.astype("timedelta64[D]")

    rng = np.random.default_rng(42)
    fluxes_data = rng.random((12, len(lat), len(lon))).astype(np.float32) * 1e-10

    ds = xr.Dataset(
        {
            "fluxes": xr.DataArray(
                fluxes_data,
                dims=["time", "lat", "lon"],
                attrs={
                    "units": "kg m-2 s-1",
                    "substance": "CH4",
                    "year": "2001",
                    "release": "EDGARv2024ghg",
                    "long_name": "Agriculture",
                    "description": "Agriculture",
                },
            )
        },
        coords={
            "lat": xr.DataArray(
                lat,
                dims=["lat"],
                attrs={"units": "degrees_north", "standard_name": "latitude", "long_name": "latitude"},
            ),
            "lon": xr.DataArray(
                lon,
                dims=["lon"],
                attrs={"units": "degrees_east", "standard_name": "longitude", "long_name": "longitude"},
            ),
            "time": xr.DataArray(
                time_dates,
                dims=["time"],
                attrs={"long_name": "time", "standard_name": "time"},
            ),
        },
        attrs={
            "description": "Agriculture",
            "institution": "European Commission, Joint Research Centre",
            "source": "https://edgar.jrc.ec.europa.eu/dataset_ghg2024",
            "how_to_cite": "https://edgar.jrc.ec.europa.eu/dataset_ghg2024#howtocite",
            "copyright_notice": "https://edgar.jrc.ec.europa.eu/dataset_ghg2024#conditions",
            "contacts": "https://edgar.jrc.ec.europa.eu/dataset_ghg2024#info JRC-EDGAR@ec.europa.eu",
            "units": "kg m-2 s-1",
        },
    )

    filepath = edgar_dir / "EDGAR_2024_GHG_CH4_2001_AGRICULTURE_flx.nc"
    ds.to_netcdf(filepath, encoding={"time": {"units": "days since 2001-01-01 00:00:00"}})

    return edgar_dir, fluxes_data


def test_parse_edgar_monthly_v8_2024(edgar_v8_monthly_dir):
    """
    Test parse_edgar with a synthetic monthly EDGAR v8 (2024) sectoral file.

    The synthetic file matches the structure of real EDGAR 2024 monthly sectoral files:
        EDGAR_2024_GHG_CH4_2001_AGRICULTURE_flx.nc

    Checks:
    - The 'fluxes' variable is read and units are converted from kg/m2/s to mol/m2/s
    - The time dimension contains 12 monthly time steps
    - Metadata fields are correctly populated (species, domain, source, database, time_period)
    - The result key follows the expected naming convention
    """
    edgar_dir, fluxes_data = edgar_v8_monthly_dir

    result = parse_edgar(edgar_dir, date="2001", species="ch4", edgar_version="v8.0")

    expected_key = "ch4_agriculture_globaledgar_2001"
    assert expected_key in result

    data_values = result[expected_key]
    data = data_values["data"]

    # Check flux variable and unit conversion
    assert "flux" in data
    assert data["flux"].attrs["units"] == "mol/m2/s"

    mol_mass = 16.0426  # g/mol for CH4
    expected_mean_flux = float(fluxes_data.mean()) * 1e3 / mol_mass
    assert np.isclose(float(data["flux"].mean()), expected_mean_flux)

    # Check time dimension has 12 monthly steps
    assert "time" in data.dims
    assert data.sizes["time"] == 12

    # Check metadata
    metadata = data_values["metadata"]
    assert metadata["species"] == "ch4"
    assert metadata["domain"] == "globaledgar"
    assert metadata["source"] == "agriculture"
    assert metadata["database"] == "EDGAR"
    assert metadata["database_version"] == "v8.0"
    assert metadata["time_period"] == "1 month"
    assert metadata["start_date"].startswith("2001-01-15")
    # end_date = last timestamp (Dec 15) + 1 month - 1 second = Jan 14 2002 23:59:59
    assert metadata["end_date"].startswith("2002-01-14")
