import math
import pytest

from openghg.util import clean_string, extract_float, is_number


@pytest.mark.parametrize(
    "input_string,cleaned_string",
    [
        ("tacol?neston", "tacolneston"),             # non-alphanumeric characters
        ("top_rated-parrot!!!", "top_rated-parrot"), # messy string
        ("eulerian_model", "eulerian_model")  ,      # model name with underscore
        ("10.0", "10.0"),                            # number with decimal
        ("10.0magl", "10.0magl"),                    # number with decimal and characters
        ("10.0 m agl", "10.0magl"),                  # number with decimal, characters and spaces
        (60.0, "60.0"),                              # float
        (-90.0, "-90.0"),                            # negative float
        (True, "true"),                              # bool
    ],
)
def test_clean_string(input_string, cleaned_string):
    """
    Check clean_string produces expected output. By default this should keep
    all alphanumeric characters and additionally underscores, dashes and full stops
    but removes spaces and other non-alphanumeric characters.

    Included checks:
     - Removes unexpected non-alphanumeric characters
     - Can handle decimal and character combinations in a string
     - Can handle floats
     - Can handle booleans as expected (lowercase and string)
    """
    assert clean_string(input_string) == cleaned_string


@pytest.mark.parametrize(
    "input_string,cleaned_string,keep_special_characters",
    [
        ("-12.0_magl", "-12.0_magl", "default"),
        ("-12.0_magl", "120magl", None),
        ("necessary?", "necessary", "default"),
        ("necessary?", "necessary?", ["?"]),
        ("  \\[--..]  ", "\\[--..]", ["\\", "-", ".", "[", "]"]),
        ("  \\[--..]  ", "", None),
    ],
)
def test_clean_string_keep(input_string, cleaned_string, keep_special_characters):
    """
    Check the keep_special_characters input for clean_string. This defines the non-alphanumeric
    characters which are allowed (check function for default - prev ["_", "-", "."])

    Included checks:
     - Impact of keep_special_characters on string with the default characters
     - Check different special characters can be included
     - Check of regex special characters (internally need to be escaped for regex substitution)
    """
    if keep_special_characters == "default":
        assert clean_string(input_string) == cleaned_string
    else:
        assert clean_string(input_string, keep_special_characters) == cleaned_string


def test_is_number():
    from numpy import nan

    assert is_number(99)
    assert is_number("-9999.999")
    assert not is_number("sparrow")
    assert is_number(nan)
    assert is_number("NaN")

    assert not is_number(False)

    assert not is_number(["999"])


@pytest.mark.parametrize(
    "string_val, float_val",
    [
        ("1234", 1234.0),
        ("1_2_3.4", 123.4),
        ("nan", float("nan")),
        ("123.456", 123.456),
        (".1", 0.1),
        ("123 bananas", 123.0),
        ("+1.23", 1.23),
        ("-1.23", -1.23),
        ("1e-2", 1e-2),
        ("1e2", 1e2),
        ("100m", 100.0),
        ("100magl", 100.0),
        ("to +inf and beyond", float("inf")),
    ],
)
def test_extract_float(string_val, float_val):
    if string_val != "nan":
        assert extract_float(string_val) == float_val
    else:
        assert math.isnan(extract_float(string_val))


def test_extract_float_exclude_bad_nan_inf():
    """Exclude cases where nan or inf occur inside a word."""
    with pytest.raises(ValueError):
        extract_float("banana")

    with pytest.raises(ValueError):
        extract_float("gainful")

    with pytest.raises(ValueError):
        extract_float("inferno")

    with pytest.raises(ValueError):
        extract_float("nana")
