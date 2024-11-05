import pytest
from openghg.util import format_data_level
from openghg.types import MetadataFormatError


@pytest.mark.parametrize(
    "test_input", ["0", "1", "1.1", "1.10", "2", "3", 1, 2.2, None],
)
def test_format_data_level(test_input):
    """Test correct values for data_level are accepted"""
    data_level = format_data_level(test_input)
    if test_input is not None:
        assert data_level == str(test_input)
    else:
        assert data_level == test_input


@pytest.mark.parametrize(
    "test_input", ["4", 4, "1.2.3", "data level 1"],
)
def test_format_data_level_incorrect(test_input):
    """Test error is raised for incorrect values"""
    with pytest.raises(MetadataFormatError) as excinfo:
        format_data_level(test_input)
        assert "Expect: '0', '1', '2', '3'" in excinfo
