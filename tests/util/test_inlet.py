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


def test_format_inlet_special():
    """
    Test new special keywords can be specified
    """
    special_keyword = "special"

    output = format_inlet(special_keyword, special_keywords=[special_keyword])
    assert output == special_keyword
