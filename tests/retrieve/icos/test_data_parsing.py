import pytest
from unittest.mock import patch

from test_queries import mock_run_query_attrs
from helpers import get_retrieval_datapath
from openghg.retrieve.icos._data_parsing import get_data_attrs, get_icos_data


current_text_data_dummy_filename = "text_data_lin_sf6_flask.txt"


def mock_text_data(dobj_uri):
    """
    This function is used to mock our get_icos_text_file() function, which returns an example
    of flask data from within an ICOS text file as a string.

    Note: dobj_uri is not used but is included to map to expected input for get_icos_text_file() function
    """
    filename = get_retrieval_datapath(filename=current_text_data_dummy_filename,
                                      archive="ICOS")
    text = open(filename, "r").read()

    return text


@pytest.fixture(scope="module")
def mock_retrieve_text_data(module_mocker):
    """
    Creates a module mocker which patches calls to our _data_parsing.get_icos_text_file() function.
    This function calls icos_core.icos.data.get_file_stream(), which returns a HTTPResponse object, but
    we have mocked the output of our function which is a text string instead as a simpler option.
    """
    module_mocker.patch("openghg.retrieve.icos._data_parsing.get_icos_text_file", new=mock_text_data)


def icos_format_info_df():
    """
    This function mocks the pandas DataFrame created by icos_format_info(). This reads from a stored
    csv file and creates a pandas DataFrame.
    """
    import pandas as pd

    icos_format_info_filename = "icos_format_info.csv"
    filename = get_retrieval_datapath(filename=icos_format_info_filename,
                                      archive="ICOS")
    df = pd.read_csv(filename, index_col="spec_label")

    return df


@pytest.fixture(scope="module")
def mock_icos_format_info(module_mocker):
    """
    Creates a module mocker which patches the call to our icos_format_info() function.
    This function calls make_query_df() --> meta.sparql_select() with a special format_query()
    designed to grab specific details about the available ICOS format options.    

    Note: this needs to be mocked where this is called so for these tests the functions we want to mock call
        icos_format_info() from within the _data_parsing submodule, rather than _queries directly,
        this needs to be mocked there.
    """
    module_mocker.patch("openghg.retrieve.icos._data_parsing.icos_format_info", new=icos_format_info_df)


@pytest.fixture(scope="module")
def mock_sparql_query_attrs(module_mocker):
    """
    Creates a module mocker which patches calls to the meta.sparql_select() 
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

    
def test_get_icos_flask_data(mock_sparql_query_attrs, mock_retrieve_text_data, mock_icos_format_info):
    """
    Check get_icos_data() function can correctly parse flask data.
     - fmt="asciiFlaskTimeSer"
    See output of icos_format_info_df() for details of how "spec_label" maps to "fmt"

    Need to mock any calls to external service (through icos_core.core.meta and icos_core.core.data):
     - get_full_attrs() --> get_data_attrs() --> mock_sparql_query_attrs
     - get_icos_text_file() --> mock_retrieve_text_data
     - icos_format_info() --> mock_icos_format_info

    TODO: Decide what other format details we would want to check for the output Dataset of this function.
    """
    dummy_data_info = {"spec_label": "ICOS ATC/CAL Flask Release",
                       "dobj_uri": "",
                       "species": "sf6"}  # minimal details needed
    ds = get_icos_data(dummy_data_info)
    
    assert ds
    assert "time" in ds


# TODO: Add get_icos_data() tests for other types of downloaded data which are supported.
# Currently, fmt values supported are:
# - asciiFlaskTimeSer - covered by test_get_icos_flask_data()
# - asciiAtcProductTimeSer
# - netcdfTimeSeries
