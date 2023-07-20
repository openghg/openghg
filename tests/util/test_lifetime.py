from pathlib import Path

import pytest
from openghg.util import check_lifetime_monthly, species_lifetime


@pytest.mark.parametrize("species,expected_lifetime",
                         [("ch4", None),
                          ("CO2", None),
                          ("Rn", "5.5157D"),
                          ("HFO1234yf", ["35.6D", "24.3D", "14.1D", "7.8D", "4.8D", "3.6D","3.7D", "4.7D", "7.9D","14.9D","30.8D","41.8D"]),
                          ]
                        )
def test_species_lifetime(species, expected_lifetime):
    """
    Testing species inputs and expected outputs
    based on "data/acrg_species_info.json" definitions
    """
    lifetime = species_lifetime(species)
    assert lifetime == expected_lifetime


@pytest.mark.parametrize("lifetime,monthly_expected",
                         [(None, False),
                          ("5.5157D", False),
                          (["35.6D", "24.3D", "14.1D", "7.8D", "4.8D", "3.6D","3.7D", "4.7D", "7.9D","14.9D","30.8D","41.8D"], True),
                          ]
                        )
def test_lifetime_monthly(lifetime, monthly_expected):
    """
    Testing lifetime monthy checker. Informs user if lifetime is formatted
    to look like monthly lifetime.
    """
    monthly = check_lifetime_monthly(lifetime)
    assert monthly == monthly_expected


def test_monthly_mismatch():
    """
    Test monthly checker raises an exception if lifetime is a list but does not
    include 12 entries.
    """
    with pytest.raises(ValueError):
        lifetime = ["12D", "13D"]
        monthly = check_lifetime_monthly(lifetime)


def test_species_info_file_mock():
    """
    Test species_info_file from openghg_defs is being successfully mocked.

    Because species_info_file is an external file (which can be updated), we have
    added a mock for an internal version when species_info_file is imported
    which is static.

    This is mocked within an autouse fixture in tests/conftest.py/openghg_defs_mock
    """
    from openghg_defs import species_info_file

    # Check local filepath is being used when external module is called.
    expected_location_end = Path("openghg/tests/data/info/species_info.json")

    assert species_info_file.parts[-5:] == expected_location_end.parts
