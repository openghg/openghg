import pytest
from openghg.util import format_inlet


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("10m", "10m"),
        ("12.2m", "12.2m"),
        ("123", "123m"),
        ("123.45", "123.5m"),
        ("10magl", "10m"),
        ("13.3magl", "13.3m"),
        ("multiple", "multiple"),
        ("column", "column"),
        (None, None),
    ],
)
def test_format_inlet(test_input, expected):
    """
    Test format_inlet formats inlet names in the right way including expected
    special keywords ("multiple") and None.
    """
    output = format_inlet(test_input)
    assert output == expected


@pytest.mark.parametrize(
    "test_input,expected,key_name",
    [
        ("10", "10m", None),
        ("12.1", "12.1m", "inlet"),
        ("10m", "10m", "inlet"),
        ("10m", "10", "inlet_m"),
        ("10m", "10", "inlet_magl"),
        ("10magl", "10", "inlet_magl"),
        ("31m", "31", "station_height_masl"),
        ("31masl", "31", "station_height_masl"),
        ("31m", "31m", "m_height"),  # Won't recognise "m" unit not at end of string
    ],
)
def test_format_inlet_keyname(test_input, expected, key_name):
    """
    Test format_inlet formats inlet names in the right way when a key_name
    is specified. The function will derive whether a unit needs to be 
    included or not.
    """
    output = format_inlet(test_input, key_name=key_name)
    assert output == expected


def test_format_inlet_special():
    """
    Test new special keywords can be specified
    """
    special_keyword = "special"

    output = format_inlet(special_keyword, special_keywords=[special_keyword])
    assert output == special_keyword
