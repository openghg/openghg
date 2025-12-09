import pytest
from unittest.mock import patch

from test_queries import mock_run_query_attrs
from openghg.retrieve.icos._data_parsing import get_data_attrs


@pytest.fixture(scope="module")
def mock_sparql_query_attrs(module_mocker):
    """
    Creates a module wide mocker which patches calls to the meta.sparql_select() 
    to use the mock_run_query_attrs() function instead.
    """
    module_mocker.patch("icoscp_core.icos.meta.sparql_select", new=mock_run_query_attrs)


def test_get_data_attrs(mock_sparql_query_attrs):
    """
    Check get_data_attrs() is able to process the query data and produce expected attr details.

    TODO: Decide if to add more specific checks (e.g. check exact values for attrs) or if checking
        that certain names and attrs are present is fine.
    e.g. could check
        assert result["NbPoints"]["dtype"] == "int32"
        assert result["NbPoints"]["long_name"] == "number of points"
    """
    uri = "1234"  # dummy value
    result = get_data_attrs(uri, species="sf6")
    
    expected_keys = ['NbPoints', 'SamplingStart', 'SamplingEnd', 'sf6', 'Flag', 'Stdev']
    assert set(expected_keys) >= set(result.keys())

    expected_attrs = ["dtype", "long_name"]

    for key in expected_keys:
        assert set(result[key].keys()) >= set(expected_attrs)

    assert "units" in result["sf6"].keys()

    


