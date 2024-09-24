import pytest
from openghg.util import get_overlap_keys, merge_dict


@pytest.mark.parametrize(
    "left,right,expected_output",
    [
        (
            {"site": "bsd", "inlet": "10m", "species": "ch4"},
            {"data_level": "1"}, 
            []
        ),
        (
            {"site": "bsd", "inlet": "10m", "species": "ch4"},
            {"site": "bsd", "inlet": "10m", "data_level": "1"}, 
            ["inlet", "site"]
        ),
        ({}, {}, []),
    ],
)
def test_get_overlap_keys(left, right, expected_output):
    """
    Test get_overlap_keys can return expected overlapping keys.
    1. Check empty list returned when no overlap
    2. Check overlap with matching keys are identified
    3. Check two empty input dictionaries return an empty list
    """
    output = get_overlap_keys(left, right)
    output.sort()
    assert output == expected_output


@pytest.mark.parametrize(
    "left,right,expected_output",
    [
        (
            {"site": "bsd", "inlet": "10m"},
            {"data_level": "1"}, 
            {"site": "bsd", "inlet": "10m", "data_level": "1"},
        ),
        (
            {"site": "bsd", "inlet": "10m"},
            {"site": "bsd", "data_level": "1"}, 
            {"site": "bsd", "inlet": "10m", "data_level": "1"},
        ),
        (
            {"site": "bsd", "inlet": "10m"},
            {"site": "BSD", "data_level": "1"}, 
            {"site": "bsd", "inlet": "10m", "data_level": "1"},
        ),
        (
            {"site": "BSD", "data_level": "1"},
            {"site": "bsd", "inlet": "10m"}, 
            {"site": "BSD", "inlet": "10m", "data_level": "1"},
        ),
        (
            {"site": "bsd", "inlet": "10m", "latitude": 56.733},
            {"latitude": "56.7330001"}, 
            {"site": "bsd", "inlet": "10m", "latitude": 56.733},
        ),
        (
            {"site": "bsd", "inlet": "10m"},
            {},
            {"site": "bsd", "inlet": "10m"},
        ),
        (
            {"site": "bsd", "inlet": "not_set"},
            {"site": "not_set", "inlet": "10m"},
            {"site": "bsd", "inlet": "10m"},
        ),
        (
            {"site": "bsd", "inlet": "not_set"},
            {"site": "not_set", "inlet": "10m", "species": "not_set"},
            {"site": "bsd", "inlet": "10m", "species": "not_set"},
        ),
        (
            {"site": "not_set", "inlet": "not_set"},
            {"site": "not_set", "inlet": "10m"},
            {"site": "not_set", "inlet": "10m"},
        ),
        (
            {"site": "bsd", "inlet": None},
            {"site": None, "inlet": "10m", "data_level": None},
            {"site": "bsd", "inlet": "10m"},
        ),       
    ],
)
def test_merge_dict(left, right, expected_output):
    """
    1. Check merge with no overlap
    2. Check merge when keys overlap with identical value
    3. Check merge when keys overlap with same str value when lower case
    4. Check merge when keys overlap with same str value when lower case (reverse order)
    5. Check merge when keys overlap and number is within tolerance
    6. Check empty dictionary can be included and original dict returned
    7. Check not set value (e.g. "not_set") can be ignored and other value used
    8. Check not set value will be retained if key doesn't overlap
    9. Check not set value will be retained if both keys have the same "not_set" value
    10. Check null value (e.g. None) can be removed and ignored.

    For 3, 4 & 5, expect value from left to be used (order matters)
    """
    output = merge_dict(left, right)
    assert output == expected_output


def test_merge_dict_raises_no_value_check():
    """Test error is raised when there is an overlapping key and value isn't checked"""
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"site": "bsd"}

    with pytest.raises(ValueError) as excinfo:
        merge_dict(left, right, on_overlap="error")

    assert "Unable to merge dictionaries with overlapping keys" in str(excinfo.value)


@pytest.mark.parametrize(
    "on_conflict,left,right,expected_output",
    [
        (
            "left",
            {"site": "bsd", "inlet": "10m"},
            {"site": "TAC"},
            {"site": "bsd", "inlet": "10m"},
        ),
        (
            "right",
            {"site": "bsd", "inlet": "10m"},
            {"site": "TAC"},
            {"site": "TAC", "inlet": "10m"},
        ),
        (
            "drop",
            {"site": "bsd", "inlet": "10m"},
            {"site": "TAC"},
            {"inlet": "10m"},
        ),      
    ],
)
def test_merge_dict_mismatch(left, right, on_conflict, expected_output):
    """
    Test value mismatch can be updated in the correct way for the on_conflict input.
    (1) "left", (2) "right", (3) "drop".
    """
    output = merge_dict(left, right, on_conflict=on_conflict)
    assert output == expected_output


def test_merge_dict_raises_mismatch():
    """Test error can be raised when there is an overlapping key and value doesn't match"""
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"site": "TAC"}

    with pytest.raises(ValueError) as excinfo:
        merge_dict(left, right, on_conflict="error")

    assert "Same key(s) supplied from different sources:" in str(excinfo.value)


def test_merge_specific_keys():
    """Test specific keys can be selected for both dict inputs when merging the dictionaries"""
    specific_keys = ["site", "inlet", "data_level"]
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"data_level": "1", "species": "inert"}
    expected_output = {"site": "bsd", "inlet": "10m", "data_level": "1"}

    output = merge_dict(left, right, keys=specific_keys)
    assert output == expected_output


def test_merge_specific_keys_left():
    """Test specific keys can be selected for left inputs when merging the dictionaries"""
    specific_keys_left = ["site", "inlet"]
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"data_level": "1", "species": "inert"}
    expected_output = {"site": "bsd", "inlet": "10m", "data_level": "1", "species": "inert"}

    output = merge_dict(left, right, keys_left=specific_keys_left)
    assert output == expected_output


def test_merge_specific_keys_right():
    """Test specific keys can be selected for right inputs when merging the dictionaries"""
    specific_keys_right = ["data_level"]
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"data_level": "1", "species": "inert"}
    expected_output = {"site": "bsd", "inlet": "10m", "species": "ch4", "data_level": "1"}

    output = merge_dict(left, right, keys_right=specific_keys_right)
    assert output == expected_output


def test_merge_not_set_values():
    """Test not_set_values so value is ignored in preference to other value"""
    not_set_values = ["nothing to see here"]
    left = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    right = {"site": "nothing to see here", "data_level": "1"}
    expected_output = {"site": "bsd", "inlet": "10m", "species": "ch4", "data_level": "1"}

    output = merge_dict(left, right, not_set_values=not_set_values)
    assert output == expected_output


def test_merge_not_set_values_ignore():
    """Test default not_set_values can be ignored."""
    not_set_values = []
    left = {"site": "not_set", "inlet": "10m", "species": "ch4"}
    right = {"site": "bsd", "data_level": "1"}
    expected_output = {"site": "not_set", "inlet": "10m", "species": "ch4", "data_level": "1"}

    output = merge_dict(left, right, not_set_values=not_set_values)
    assert output == expected_output


def test_merge_null_values():
    """Test null_values so value is ignored in preference to other value"""
    null_values = ["null"]
    left = {"site": "bsd", "inlet": "10m", "species": "null"}
    right = {"site": "null", "data_level": "null"}
    expected_output = {"site": "bsd", "inlet": "10m"}

    output = merge_dict(left, right, null_values=null_values)
    assert output == expected_output


def test_merge_null_values_ignore():
    """Test null values can be made to remain if remove_null is set to False."""
    remove_null = False
    left = {"site": None, "inlet": "10m"}
    right = {"site": "bsd", "data_level": "1"}
    expected_output = {"site": None, "inlet": "10m", "data_level": "1"}
    
    output = merge_dict(left, right, remove_null=remove_null)
    assert output == expected_output
