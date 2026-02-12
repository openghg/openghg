from helpers.helpers import get_retrieval_datapath

# TODO: Add additional `create_` functions for
# - mock_text_data - current_text_data_dummy_filename = "data/retrieve/icos/text_data_lin_sf6_flask.txt"
# - icos_format_info_df - icos_format_info_filename = "data/retrieve/icos/icos_format_info.csv"


current_sparql_data_dummy_filename = "sparql_results_data_lin_sf6_example.txt"
current_sparql_attrs_dummy_filename = "sparql_results_attrs_lin_sf6_example.txt"


def create_icos_test_file(sparql_results,
                          overwrite=False,
                          test_filename=None,
                          dummy_filename=None):
    """
    Create new ICOS test file and put within appropriate directory.
    Args:
        sparql_results: SPARQLResults object returned from call to icos_core.meta.sparql_select
        overwrite: Whether to replace current mock file directly or create new file.
        test_filename: Name of new test file
        dummy_filename: Name of current dummy file
    Returns:
        None
    """
    if overwrite and not test_filename:
        test_filename = dummy_filename
        print(f"Overwriting current SPARQL results file with new search: {test_filename}")
    elif not test_filename:
        test_filename = dummy_filename.replace(".txt", "_new.txt")
    
    test_filename = get_retrieval_datapath(filename=test_filename, archive="ICOS")
    test_file = open(test_filename, "w")

    if not overwrite:
        print(f"Writing new SPARQL results into new file: {test_filename}")
        print(f"To use this new file, overwrite current '{dummy_filename}' file (or update `dummy_sparql_results` fixture).")

    test_file.write(repr(sparql_results))


def create_icos_sparql_data_test_file(site="LIN",
                                      species="sf6",
                                      overwrite=False,
                                      test_filename=None):
    """
    Create new ICOS SPARQLResults "data" file based on sparql call to icos_core service.
    This can be used for mocking this call.
    Args:
        site, species: Input site and species name to use in new data_query() search
        overwrite: Whether to replace current mock file directly or create new file.
        test_filename: Name of new test file
    Returns:
        None
        Writes to file
    """
    from openghg.retrieve.icos._queries import data_query
    from icoscp_core.icos import meta

    query = data_query(site=site, species=species)
    sparql_results = meta.sparql_select(query)

    create_icos_test_file(sparql_results,
                     overwrite, 
                     test_filename,
                     current_sparql_data_dummy_filename)


def create_icos_sparql_attrs_test_file(site="LIN",
                                       species="sf6",
                                       overwrite=False,
                                       test_filename=None):
    """
    Create new ICOS SPARQLResults "attrs" file based on sparql call to icos_core service.
    This can be used for mocking this call.
    Args:
        site, species: Input site and species name to use in new data_query() search
            to retrieve the attributes using `attrs_query()`
        overwrite: Whether to replace current mock file directly or create new file.
        test_filename: Name of new test file
    Returns:
        None
        Writes to file
    """
    from openghg.retrieve.icos._queries import make_query_df, data_query, attrs_query
    from icoscp_core.icos import meta

    query = data_query(site=site, species=species)
    query_df = make_query_df(query)
    uri = query_df["dobj_uri"][0]
    query_for_attrs = attrs_query(uri)
    sparql_results = meta.sparql_select(query_for_attrs)

    create_icos_test_file(sparql_results,
                     overwrite, 
                     test_filename,
                     current_sparql_attrs_dummy_filename)


if __name__=="__main__":

    # TODO: Add command line args for this to allow choices to be made

    create_icos_sparql_data_test_file()
    create_icos_sparql_attrs_test_file()