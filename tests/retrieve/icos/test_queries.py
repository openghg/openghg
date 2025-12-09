import pytest
from icoscp_core.sparql import SparqlResults, BoundLiteral, BoundUri

from openghg.retrieve.icos._queries import make_query_df
from helpers import get_retrieval_datapath


current_sparql_data_dummy_filename = "sparql_results_data_lin_sf6_example.txt"
current_sparql_attrs_dummy_filename = "sparql_results_attrs_lin_sf6_example.txt"


def mock_run_query_attrs(query):
    """
    This function is used to mock the run_meta_query() function, creating
    a SparqlResults object based on a previous output
    Note: query included as an input to match to the format of the run_meta_query()
    function but is not used at the moment.
    """

    filename = get_retrieval_datapath(filename=current_sparql_attrs_dummy_filename,
                                      archive="ICOS")
    
    results_str = open(filename, "r").read()
    sparql_results = eval(results_str)

    return sparql_results


@pytest.fixture()
def mock_sparql_query_attrs(mocker):
    """
    Creates a module wide mocker which patches calls to the meta.sparql_select() 
    to use the mock_run_query_attrs() function instead.
    """
    mocker.patch("icoscp_core.icos.meta.sparql_select", new=mock_run_query_attrs)
