import pytest
from icoscp_core.sparql import SparqlResults, BoundLiteral, BoundUri

from openghg.retrieve.icos._queries import make_query_df
from helpers import get_retrieval_datapath

# TODO: Check if we need two example files
# - attrs_query is used for get_data_attrs()
#    - returns variable_names=['col_label', 'p_label', 'o_label', 'o', 'unit']
# - data_query is used for get_icos_data()
#    - returns variable_names=['site', 'inlet', 'species', 'data_level', 'file_name', 'spec_label', 'project_name', 'dobj_uri']


current_sparql_data_dummy_filename = "sparql_results_data_lin_sf6_example.txt"
current_sparql_attrs_dummy_filename = "sparql_results_attrs_lin_sf6_example.txt"


# # If can't import helpers can use
# def get_retrieval_datapath(filename: str, archive = str | None):
#     from pathlib import Path
#     if archive:
#         return Path(__file__).parent.parent.parent.joinpath(f"data/retrieve/{archive.upper()}/{filename}").resolve()
#     else:
#         return Path(__file__).parent.parent.parent.joinpath(f"data/retrieve/{filename}").resolve()

# def create_test_file(sparql_results,
#                      overwrite=False,
#                      test_filename=None,
#                      dummy_filename=None):

#     if overwrite and not test_filename:
#         test_filename = dummy_filename
#         print(f"Overwriting current SPARQL results file with new search: {test_filename}")
#     elif not test_filename:
#         test_filename = dummy_filename.replace(".txt", "_new.txt")
    
#     test_filename = get_retrieval_datapath(filename=test_filename, archive="ICOS")
#     test_file = open(test_filename, "w")

#     if not overwrite:
#         print(f"Writing new SPARQL results into new file: {test_filename}")
#         print(f"To use this new file, overwrite current '{dummy_filename}' file (or update `dummy_sparql_results` fixture).")

#     test_file.write(repr(sparql_results))


# def create_sparql_data_test_file(site="LIN",
#                                  species="sf6",
#                                  overwrite=False,
#                                  test_filename=None):
#     """
#     NEED TO PUT THIS SOMEWHERE TO RUN WHERE THIS CAN ACCESS HELPERS
#     """
#     from openghg.retrieve.icos._queries import data_query
#     from icoscp_core.icos import meta

#     query = data_query(site=site, species=species)
#     sparql_results = meta.sparql_select(query)

#     create_test_file(sparql_results,
#                      overwrite, 
#                      test_filename,
#                      current_sparql_data_dummy_filename)


# def create_sparql_attrs_test_file(site="LIN",
#                                   species="sf6",
#                                   overwrite=False,
#                                   test_filename=None):
#     """
#     NEED TO PUT THIS SOMEWHERE TO RUN WHERE THIS CAN ACCESS HELPERS
#     """
#     from openghg.retrieve.icos._queries import make_query_df, data_query, attrs_query
#     from icoscp_core.icos import meta

#     query = data_query(site=site, species=species)
#     query_df = make_query_df(query)
#     uri = query_df["dobj_uri"][0]
#     query_for_attrs = attrs_query(uri)
#     sparql_results = meta.sparql_select(query_for_attrs)

#     create_test_file(sparql_results,
#                      overwrite, 
#                      test_filename,
#                      current_sparql_attrs_dummy_filename)


def mock_run_query_attrs(query):
    """
    This function is used to mock the run_meta_query() function, creating
    a SparqlResults object based on a previous output

    The string in the file looks like:
    "SparqlResults(variable_names=...,
                   bindings=[{...: BoundLiteral(value='...', datatype=...),
                              ...: BoundUri(uri='...')
                              }])"
    and we have imported SparqlResults, BoundLiteral, BoundUri so these objects can
    be correctly evaluated to create this object.

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


def test_make_query_df(mock_sparql_query_attrs):
    results = make_query_df(query="")
    print(results)
    # TODO: Add actual check!


# if __name__=="__main__":

#     # TODO: Add command line args for this
#     # TODO: Update helpers function to allow filepath to be created from further nested directory.
#     # - may want to put in as small tidy PR

#     create_sparql_data_test_file()
#     create_sparql_attrs_test_file()
