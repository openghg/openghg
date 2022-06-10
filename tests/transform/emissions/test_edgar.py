import pytest
import numpy as np
from helpers import get_emissions_datapath
from openghg.transform.emissions import parse_edgar
from openghg.transform.emissions._edgar import _extract_file_info


@pytest.mark.parametrize("folder,version,mean_raw_flux",
                         [("v50", "v50", 2.261304e-11),
                          ("v6.0", "v6.0", 2.0385315e-11),
                          ("TOTALS_nc.zip", "v6.0", 2.0385315e-11),
                         ])
def test_parse_edgar_raw(folder, version, mean_raw_flux):
    """
    Test parse edgar function against different database options.
    """
    filepath = get_emissions_datapath(f"EDGAR/yearly/{folder}")

    species = "ch4"
    year = "2015"

    entry = parse_edgar(filepath, date=year, species=species)

    data_values = list(entry.values())[0]
    data = data_values["data"]

    # Raw flux from EDGAR file is in kg/m2/s - new units are mol/m2/s
    ch4_mol_mass = 16.0426  # g/mol
    mean_converted_flux = mean_raw_flux * 1e3 / ch4_mol_mass

    variable = "flux"
    assert variable in data
    assert np.isclose(data[variable].mean(), mean_converted_flux)
    assert data[variable].attrs["units"] == "mol/m2/s"

    # attrs = data.attrs

    default_domain = "globaledgar"

    expected_metadata = {
        "species": species,
        "domain": default_domain,
        "source": "anthro",
        "database": "EDGAR",
        "database_version": version.replace('.',''),
        "date": "2015",
        "author": "OpenGHG Cloud",
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        "min_longitude": 0.05,
        "max_longitude": 359.95001,
        "min_latitude": -89.95,
        "max_latitude": 89.95,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    metadata = data_values["metadata"]
    assert metadata.items() >= expected_metadata.items()


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
                         ])
def test_extract_file_info(edgar_file, expected_file_info):
    """Test that the expected file information can be extracted from the EDGAR filename."""
    file_info = _extract_file_info(edgar_file)
    assert file_info == expected_file_info
