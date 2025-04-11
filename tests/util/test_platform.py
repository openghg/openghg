import pytest
from openghg.util import define_platform, format_platform
from openghg.types import MetadataFormatError


@pytest.mark.parametrize(
    "data_type, value",
    [
        ("surface", "surface-flask"),
        ("column", "column-insitu"),
        ("column", "satellite"),
        ("mobile", "mobile-flask"),
    ],
)
def test_define_platform(data_type, value):
    """
    Check selected values are returned from define_platform when specifying a data_type
    """
    output = define_platform(data_type=data_type)
    assert value in output


def test_define_platform_incorrect_datatype():
    """
    Check known values from other data types are not returned from define_platform
    when specifying a data_type
    """
    data_type = "column"
    not_expected = "surface-flask"
    output = define_platform(data_type=data_type)
    assert not_expected not in output


def test_format_platform_allowed():
    """
    Test all known values for platform are accepted by format_platform.
    """
    allowed_platform_values = define_platform()

    for test_input in allowed_platform_values:
        output = format_platform(test_input)
        assert output == test_input


@pytest.mark.parametrize(
    "test_input",
    [
        None,
        "not_set",
        "Surface-Insitu",
    ],
)
def test_format_platform_extra(test_input):
    """
    Test additional values which can be used for platform are acceepted and formatted
    as expected.
    1. Check None is an accepted value
    2. Check "not_set" is an accepted value
    3. Check correct values but with upper case are accepted (returned to match the stored value)
    """
    output = format_platform(test_input)

    if test_input is not None:
        assert output == test_input.lower()
    else:
        assert output == test_input


@pytest.mark.parametrize(
    "test_input",
    ["site-insitu", "upside-down"],
)
def test_format_platform_incorrect(test_input):
    """Test error is raised for unrecognised values"""
    with pytest.raises(MetadataFormatError) as excinfo:
        format_platform(test_input)
        assert "This must be one of:" in excinfo
