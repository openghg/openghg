import pytest
from openghg.util import check_overlap_keys


@pytest.mark.parametrize(
    "dict1,dict2,expected_output",
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
        (
            {"site": "bsd", "inlet": "10m", "species": "ch4"},
            {"site": "BSD", "data_level": "1"}, 
            ["site"]
        ),
        (
            {"site": "bsd", "inlet": "10m", "latitude": 56.733},
            {"latitude": "56.73300000001"}, 
            ["latitude"]
        ),
    ],
)
def test_check_overlap_keys(dict1, dict2, expected_output):
    """
    Test check_overlap_keys can return overlapping keys and will allow
    tolerance around numbers and string cases by default.
    1. Check empty list returned when no overlap
    2. Check overlap with matching keys are identified (same value)
    3. Check overlap with matching key is identified (lower case match)
    4. Check overlap with matching key is identified (number in tolerance)
    """
    output = check_overlap_keys(dict1, dict2)
    output.sort()
    assert output == expected_output


def test_check_overlap_key_raises():
    """Test error is raised when there is an overlapping key and value doesn't match"""
    dict1 = {"site": "bsd", "inlet": "10m", "species": "ch4"}
    dict2 = {"site": "TAC"}

    with pytest.raises(ValueError) as excinfo:
        check_overlap_keys(dict1, dict2)
        assert "Same key(s) supplied from different sources:" in excinfo

