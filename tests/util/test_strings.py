import math

import pytest

from openghg.util import clean_string, extract_float, is_number


def test_clean_string():
    dirty_string = "tacol?neston"

    assert clean_string(dirty_string) == "tacolneston"

    number = 60.0

    assert clean_string(number) == "60.0"

    negative = -90.0

    assert clean_string(negative) == "-90.0"

    messy_string = "top_rated-parrot!!!"

    assert clean_string(messy_string) == "top_rated-parrot"

    model_type = "eulerian_model"

    assert clean_string(model_type) == "eulerian_model"


def test_is_number():
    from numpy import NaN

    assert is_number(99)
    assert is_number("-9999.999")
    assert not is_number("sparrow")
    assert is_number(NaN)
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
