import pytest
from openghg.util import clean_string, is_number


def test_clean_string():
    dirty_string = "tacol-neston"

    assert clean_string(dirty_string) == "tacolneston"

    number = 60.0

    assert clean_string(number) == "60.0"

    negative = -90.0

    assert clean_string(negative) == "-90.0"

    messy_string = "top_rated-parrot!!!"

    assert clean_string(messy_string) == "top_ratedparrot"

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

    with pytest.raises(TypeError):
        is_number(["999"])
