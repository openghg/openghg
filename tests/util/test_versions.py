import pytest

from openghg.util._versions import check_if_need_new_version


@pytest.mark.parametrize(
    "if_exists, save_current, expected",
    [
        ("auto", "auto", False),
        ("new", "auto", True),
        ("combine", "auto", True),
        ("auto", "y", True),
        ("auto", "yes", True),
        ("combine", "n", False),
        ("new", "no", False),
    ],
)
def test_check_if_need_new_version(if_exists, save_current, expected):
    assert check_if_need_new_version(if_exists, save_current) is expected


@pytest.mark.parametrize(
    "if_exists, save_current, match",
    [
        ("replace", "auto", "Invalid if_exists"),
        ("new", False, "Invalid save_current"),
    ],
)
def test_check_if_need_new_version_rejects_invalid_options(if_exists, save_current, match):
    with pytest.raises(ValueError, match=match):
        check_if_need_new_version(if_exists, save_current)
