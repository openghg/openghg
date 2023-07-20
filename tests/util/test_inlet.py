import pytest
from openghg.util import extract_height_name, format_inlet


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
        (10, "10m"),
        (10.0, "10m"),
        (20.23456, "20.2m"),
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
        ("31m", "31m", "m_height"),  # Correctly won't recognise "m" unit in key_name as not at end of string
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


@pytest.mark.parametrize(
    "site,network,inlet,expected",
    [
        ("POCN25", None, None, None),  # height_name not present
        ("BAO", None, None, "300magl"),  # 1 network, 1 height_name value
        ("MHD", "AGAGE", None, "10magl"),  # 1 height_name value
        ("cgo", "AGAGE", "10m", "10magl"),  # Multiple height_name values
        ("WAO", "ICOS", "10m", ["10magl", "20magl"]),  # height_name is dictionary
    ],
)
def test_extract_height_name(site, network, inlet, expected):
    """
    Test 'height_name' variable can be extracted from site_info file.
    """
    height_name = extract_height_name(site, network, inlet)

    assert height_name == expected
