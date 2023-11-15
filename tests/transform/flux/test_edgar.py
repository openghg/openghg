import numpy as np
import pytest
from helpers import get_flux_datapath
from openghg.transform.flux import parse_edgar
from openghg.transform.flux._edgar import _extract_file_info

# TODO: Add tests
# - single sector EDGAR data can be read and intepreted correctly for v6.0
#   - only monthly grid maps available?

@pytest.mark.parametrize("folder,version,species,mean_raw_flux",
                         [("v50", "v50", "ch4", 2.261304e-11),
                          ("v6.0_CH4", "v6.0", "ch4", 2.0385315e-11),
                          ("v6.0_N2O", "v6.0", "n2o", 3.8895116e-13),
                          ("v6.0_CO2_excl_shortcycle", "v6.0", "co2", 1.1799942e-09),
                          ("v6.0_CO2_org_shortcycle", "v6.0", "co2", 2.4354319e-10),
                          ("TOTALS_nc.zip", "v6.0", "ch4", 2.0385315e-11),
                         ])
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
        mol_mass = 44.01    # g/mol
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
        source_start = '_'.join(folder.split('_')[-2:])
        source_start = source_start.replace("shortcycle", "short-cycle")
        source = f"{source_start}_anthro"
    else:
        source = "anthro"

    expected_metadata = {
        "species": species,
        "domain": default_domain,
        "source": source,
        "database": "EDGAR",
        "database_version": version.replace('.',''),
        "date": "2015",
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
    domain_lat, domain_lon = find_domain(domain)

    np.testing.assert_array_equal(data["lat"].values, domain_lat)
    np.testing.assert_array_equal(data["lon"].values, domain_lon)

    version = folder.split('_')[0]

    expected_metadata = {
        "species": species,
        "domain": domain,
        "source": "anthro",
        "database": "EDGAR",
        "database_version": version.replace('.',''),
        "date": "2015",
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

    entry = parse_edgar(filepath, date=year, species=species,
                        domain=domain, lat_out=lat_out, lon_out=lon_out)

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
        parse_edgar(filepath, date=year, species=species,
                    lat_out=lat_out, lon_out=lon_out)


@pytest.mark.parametrize("edgar_file,expected_file_info",
                         [("v6.0_CH4_2015_TOTALS.0.1x0.1.nc",
                           {"version": "v6.0", "species": "CH4", "year": 2015,
                            "source": "TOTALS", "resolution": "0.1x0.1"}),
                          ("v50_CH4_2015.5.3x5.1.nc",
                           {"version": "v50", "species": "CH4", "year": 2015,
                            "source": "", "resolution": "5.3x5.1"}),
                          ("v432_CH4_2010_9_IPCC_6A_6D.0.1x0.1.nc",
                           {"version": "v432", "species": "CH4", "year": 2010,
                            "month": 9, "source": "IPCC-6A-6D",
                            "resolution": "0.1x0.1"}),
                          ("v6.0_CO2_excl_short-cycle_org_C_2000_TOTALS.0.1x0.1.nc",
                           {"version": "v6.0", "species": "CO2", "year": 2000,
                            "source": "excl_short-cycle_TOTALS",
                            "resolution": "0.1x0.1"}),
                          ("v6.0_CO2_org_short-cycle_C_1970_TOTALS.0.1x0.1.nc",
                           {"version": "v6.0", "species": "CO2", "year": 1970,
                            "source": "org_short-cycle_TOTALS",
                            "resolution": "0.1x0.1"}),
                         ])
def test_extract_file_info(edgar_file, expected_file_info):
    """Test that the expected file information can be extracted from the EDGAR filename."""
    file_info = _extract_file_info(edgar_file)
    assert file_info == expected_file_info
