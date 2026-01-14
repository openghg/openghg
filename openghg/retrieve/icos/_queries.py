"""Function for building Sparql queries for the ICOS Carbon Portal.

To experiment with these queries, call a query building function,
print the result, and paste it into the ICOS Sparql endpoint:
https://meta.icos-cp.eu/sparqlclient/?type=CSV

To run these queries programmatically, we use `meta.sparql_select`,
where `meta` is imported from `icoscp_core.icos`.
This function returns two values: a list of variable names (the columns
in the CSV output from on the SPARQL endpoint), and a list of "Bindings".
The `parse_bindings` function in this module will convert this list of bindings
into a dictionary.

The ICOS `meta` repo has some info on getting started with SPARQL queries:
 https://github.com/ICOS-Carbon-Portal/meta?tab=readme-ov-file#advanced-metadata-access-with-sparql

The wikibook on SPARQL is also helpful: https://en.wikibooks.org/wiki/SPARQL

A few notes on query *strings*:

To include brackets {} inside a f-string, you need to double them
to distinguish from brackets used to insert a value.
For instance

>>> name = "Bob"
>>> f"{{ {name} }}"
'{ Bob }'

To quote a string literal in a SPARQL query, you must use double-quotes.
"""

# from collections import defaultdict
from functools import lru_cache

# import re

from icoscp_core.icos import meta
import pandas as pd

from openghg.util import extract_float


def sparql_header() -> str:
    """Define header for SPARQL query"""
    return """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    """


def make_spec_filter(spec_label: list[str] | str | None = None, include: list[bool] | bool = True) -> str:
    """
    Create the filter for the spec_label field. For ICOS queries this is often
    searched for more complex or multiple details.

    Args:
        spec_label: regex or regexes to filter the "spec label" by. If an ordinary string
            is passed, only spec labels that contain that string as a substring will
            be matched.
        include: for spec_label this includes a regex/string search.
            The include input specifies whether the spec_label value or values
            should be included or excluded in the search.
            Examples:
            - spec_label = "CO2", spec_label_include = True
                - search for spec_label which includes "CO2"
            - spec_label = ["CO2", "time"], spec_label_include = True
                - search for spec_label which includes "CO2" AND "time"
            - spec_label = ["CO2", "Obspack"], spec_label_include = [True, False]
                - search for spec_label which includes "CO2" AND excludes "Obspack"
            Default = True
            Only used if spec_label is specified (not None).
    Returns:
        str: case-insensitive SPARQL query filter related to the spec_label field.
    Examples:
        >>> make_spec_filter("CO2")
            'FILTER(REGEX(?specLabel, "CO2", "i"))'
        >>> make_spec_filter(r"\\b(?:CO2|CH4|CO)\\b")
            'FILTER(REGEX(?specLabel, "\\\\b(?:CO2|CH4|CO)\\\\b", "i"))'
        >>> make_spec_filter(["CO2", "Obspack"])
            'FILTER(REGEX(?specLabel, "CO2", "i") && REGEX(?specLabel, "ObsPack", "i"))'
        >>> make_spec_filter(["CO2", "ObsPack"], include=False)
            'FILTER(!REGEX(?specLabel, "CO2", "i") && !REGEX(?specLabel, "ObsPack", "i"))'
        >>> make_spec_filter(["CO2", "ObsPack"], include=[True, False])
            'FILTER(REGEX(?specLabel, "CO2", "i") && !REGEX(?specLabel, "ObsPack", "i"))'
    """
    if spec_label is None:
        return ""

    if isinstance(spec_label, str):
        spec_label = [spec_label]

    if isinstance(include, bool):
        include = [include] * len(spec_label)

    filter_str = r"FILTER("
    for i, label in enumerate(spec_label):
        # Include NOT (!) condition to exclude entries which match to this value
        if include[i] is False:
            filter_str += "!"

        filter_str += rf'REGEX(?specLabel, "{label}", "i")'

        # If there are still more conditions, include an AND (&&) condition
        if i + 1 < len(spec_label):
            filter_str += " && "
    filter_str += ")"

    return filter_str


def make_filename_filter(filename_str: str | None = None) -> str:
    """
    Create a regex filter for the filename itself. This should not often be needed
    as the other parameters should provide more granular filtering but
    on occasion it may be necessary to use details within the filename as a filter.

    Args:
        filename_str: regex to filter the "file_name" by. If an ordinary string
            is passed, only filenames that contain that string as a substring will
            be matched.
    Returns:
        str: case-insensitive SPARQL query regex filter related to the file_name field.
    """
    if filename_str is None:
        return ""

    filter_str = rf'FILTER(REGEX(?file_name, "{filename_str}", "i"))'
    return filter_str


def make_site_filter(site: str | list[str] | None) -> str:
    """
    Create direct values query for the site code or list of site codes.

    Args:
        site: ICOS site code or codes as a list
    Returns:
        str: SPARQL query for site filter
    """
    if site is None:
        return ""

    site = [site] if isinstance(site, str) else site
    site = [f'"{s.upper()}"' for s in site]
    sites = " ".join(site)
    site_filt = f"VALUES ?stationId {{ {sites} }}"
    return site_filt


def make_data_level_filter(data_level: int | None) -> str:
    """
    Create filter for the data_level value. For more details see
    see https://www.icos-cp.eu/data-services/data-collection/data-levels-quality

    Args:
        data_level: data level value as an integer. This should usually be:
            - 1: intermediate observational data
            - 2: final quality controlled observational data
            In principle this can also be 0 (raw data) or 3 (elaborated product)
            but these are not typically used for this kind of observation data.
    Returns:
        str: SPARQL filter query for the data_level value
    """
    if data_level is None:
        return ""
    return f"FILTER(?data_level = {data_level})"


def make_species_filter(species: str | list[str] | None) -> str:
    """
    Create filter for species or list of species values. Note: this applies lowercase to the species
    column before comparing against the supplied species values.

    Args:
        species: species value or values
    Returns:
        str: SPARQL query for species filter
    """
    if species is None:
        return ""

    if isinstance(species, str):
        species = [species]
    species = [s.lower() for s in species]

    # sparql requires double quotes for strings, so we can't just use f"... {tuple(species)}"
    formatted_tuple = "(" + ", ".join(f'"{s}"' for s in species) + ")"
    species_filt = f"""
        # filter species
        FILTER(LCASE(?species) in {formatted_tuple})
    """
    return species_filt


def make_project_filter(project: str | list[str] | None) -> str:
    """
    Create filter for project name or list of project names.
    This label distiguishes official "icos" project data from other projects which are also hosted on the ICOS CP.
    See https://meta.icos-cp.eu/ontologies/cpmeta/Project for list of case-sensitive project names.

    Args:
        project: project name or list of project names to include in the search
    Returns:
        str: SPARQL query for project filter
    """
    if project is None:
        return ""

    if isinstance(project, str):
        project = [project]

    # sparql requires double quotes for strings, so we can't just use f"... {tuple(species)}"
    formatted_tuple = (
        "(" + ", ".join(f"<http://meta.icos-cp.eu/resources/projects/{p}>" for p in project) + ")"
    )
    project_filt = f"""
        # filter project
        ?spec cpmeta:hasAssociatedProject ?project .
        FILTER(?project in {formatted_tuple})
    """
    return project_filt


def make_inlet_filter(inlet: str | list[str] | None) -> str:
    """
    Create filter for inlet or list of inlet values. This will extract the numerical value(s) from
    the inlet and use to search the "sampling_height" column.

    Args:
        inlet: inlet as a string (e.g. "10m") or list of strings.
    Returns:
        str: SPARQL query for inlet value filter
    """
    if inlet is None:
        return ""

    if isinstance(inlet, str):
        inlet = [inlet]

    # extract floats
    try:
        inlet_float = [extract_float(i) for i in inlet]
    except ValueError as e:
        raise ValueError("Could not extract float value from inlet.") from e

    inlet_filt_statements = [f'?samplingHeight = "{i}"^^xsd:float' for i in inlet_float]
    inlet_filt = f"FILTER( {'||'.join(inlet_filt_statements)} )"
    return inlet_filt


def data_query(
    site: str | list[str] | None = None,
    data_level: int | None = None,
    species: str | list[str] | None = None,
    inlet: str | list[str] | None = None,
    spec_label: str | list[str] | None = None,
    spec_label_include: bool | list[bool] = True,
    filename_str: str | None = None,
    project: str | list[str] | None = ["icos", "euroObspack"],
    custom_filter: str = "",
    strict: bool = True,
) -> str:
    """Search ICOS CP for data by

    Args:
        site: site or list of sites to search (e.g. "MHD" or ["JFJ", "CMN"]); if
        None, search all sites.
        data_level: ICOS data level: 1, 2 or None for both.
        species: species or list of species to search for.
        inlet: inlet or list of inlets. Only exact matches are found. The inlets
          should be strings, since this is OpenGHG convention, but "100", "100.0",
          "100m" will all work.
        spec_label: regex or regexes to filter the "spec label" by. If an ordinary string
          is passed, only spec labels that contain that string as a substring will
          be matched.
        spec_label_include: For spec_label this includes a regex/string search.
            The spec_label_include input specifies whether the spec_label value or values
            should be included or excluded in the search.
            Examples:
            - spec_label = "CO2", spec_label_include = True
                - search for spec_label which includes "CO2"
            - spec_label = ["CO2", "time"], spec_label_include = True
                - search for spec_label which includes "CO2" AND "time"
            - spec_label = ["CO2", "Obspack"], spec_label_include = [True, False]
                - search for spec_label which includes "CO2" AND excludes "Obspack"
            Default = True
            Only used if spec_label is specified (not None).
        filename_str: regex to filter the "file_name" by. If an ordinary string
            is passed, only filenames that contain that string as a substring will
            be matched.
        project: list of case-sensitive project names as defined here:
          https://meta.icos-cp.eu/ontologies/cpmeta/Project
        custom_filter: text added directly to the end of the SPARQL query string
        strict: If False, don't try to check that the species found is correct.
          By default, this is True, and we try to check that the inferred species
          is present in some other type of metadata.

    Returns:
        str: SPARQL query string.
    """
    site_filt = make_site_filter(site)
    data_level_filt = make_data_level_filter(data_level)
    species_filt = make_species_filter(species)
    inlet_filt = make_inlet_filter(inlet)
    spec_filt = make_spec_filter(spec_label, spec_label_include)
    filename_filt = make_filename_filter(filename_str)
    project_filt = make_project_filter(project)

    if strict:
        strict_filt = """
        # filter to link dobj and varName
        # netCDF data doesn't seem to have the ?colNames variable, so we
        # need to detect it by checking if ?varName = "value"
        # additonal sources are found by looking at file names and the spec
        # label; this can produce a lot of NRT results, so it is best
        # to use `nrt=False` (the default)
        OPTIONAL {{ ?dobj cpmeta:hasActualColumnNames ?colNames . }}
        OPTIONAL {{ ?dobj cpmeta:hasVariableName ?actVar . }}
        FILTER(
            COALESCE(CONTAINS(?colNames, concat('"', ?varName, '"')), FALSE)
            || COALESCE(?varName = ?actVar, FALSE)
            # else, check that the variables ?colNames and ?actVar were not found
            # and then either check that ?varName is "value" or that the species
            # is found in the file name or spec label
            || ((!BOUND(?colNames) && !BOUND(?actVar))
                 && (?varName = "value"
                     || CONTAINS(LCASE(STR(?file_name)), ?species)
                     || CONTAINS(LCASE(STR(?specLabel)), ?species)))
        )
        """
    else:
        strict_filt = ""

    query = sparql_header()
    query += "SELECT (?stationId as ?site) (?samplingHeight as ?inlet) ?species ?data_level ?file_name (?specLabel as ?spec_label) (?project as ?project_name) (?dobj as ?dobj_uri)\n"

    query += f"""
    WHERE {{
        # filter to select only data without a subsequent version
        FILTER NOT EXISTS {{[] cpmeta:isNextVersionOf ?dobj}}

        # Filter empty entries (e.g. failed uploads)
        FILTER EXISTS {{ ?dobj cpmeta:hasSizeInBytes ?anyValue }}

        # get station ID (e.g. TAC, JFJ, etc)
        ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
        ?station cpmeta:hasStationId ?stationId .  # ICOS doesn't like if this is chained with the previous query
        {site_filt}

        # get object spec
        ?dobj cpmeta:hasObjectSpec ?spec .

        # get data level
        ?spec cpmeta:hasDataLevel ?data_level .
        {data_level_filt}

        # spec label
        ?spec rdfs:label ?specLabel .
        {spec_filt}

        # get sampling height
        ?dobj cpmeta:wasAcquiredBy/cpmeta:hasSamplingHeight ?samplingHeight .
        {inlet_filt}

        # get dataset
        ?spec cpmeta:containsDataset ?ds .

        # filter out meteorological data
        FILTER(?ds != <http://meta.icos-cp.eu/resources/cpmeta/atcMeteoTimeSer>)

        # get species from column names
        ?ds cpmeta:hasColumn ?col .
        FILTER EXISTS {{[] cpmeta:isQualityFlagFor ?col}}
        ?col cpmeta:hasColumnTitle ?varName.

        # get file name
        ?dobj cpmeta:hasName ?file_name .
        {filename_filt}

        # define species name: this is either ?varName or
        # the first part of the filename if ?varName = "value"
        # the species extracted from filename is probably less
        # reliable
        BIND(STRBEFORE(STR(?file_name), "_") AS ?first_part)
        BIND(
            IF(?varName = "value", ?first_part, ?varName) as ?species
        )

        {species_filt}

        {project_filt}

        {strict_filt}

        {custom_filter}

    }}
    """
    return query


def attrs_query(dobj_uri: str) -> str:
    """
    This creates a query to extract the following columns for a data object:
    - col_label - details of column name e.g. "NbPoints"
    - p_label - "value_format", "value_type", "is a quality flag for"
    - o_label - description of the type or variable e.g. "32-bit integer value", "number of points"
    - o - uri to variable details
    - unit - unit details (when present)
    These can be used to create attributes for variables.

    Args:
        dobj_uri: data URI (Uniform Resource Identifier)
    Returns:
        str: full sparql query as a string
    """
    query = sparql_header()
    query += f"""
    SELECT ?col_label ?p_label ?o_label ?o ?unit
        WHERE {{
            VALUES ?dobj {{ <{dobj_uri}> }}
            ?dobj cpmeta:hasObjectSpec ?spec .
            ?spec cpmeta:containsDataset ?ds .
            ?ds cpmeta:hasColumn ?col .
            ?col cpmeta:hasColumnTitle ?col_label .
            ?col ?p ?o .
            ?p rdfs:label ?p_label .
            ?o rdfs:label ?o_label .
            OPTIONAL {{?col cpmeta:hasValueType/cpmeta:hasUnit ?unit}}
            # filter to link dobj and col_label
            OPTIONAL {{ ?dobj cpmeta:hasActualColumnNames ?colNames . }}
            FILTER(CONTAINS(?colNames, CONCAT('"', ?col_label, '"')) || !BOUND(?colNames))
    }}
    """
    return query


def format_query() -> str:
    """
    This creates a query to extract the details of the ICOS formats.
    Returns:
        str: full sparql query as a string
    """
    query = sparql_header()
    query += """
    SELECT DISTINCT ?format_label ?spec_label ?encoding ?format ?spec ?format_comment ?spec_comment
    WHERE{
        ?format ?p cpmeta:ObjectFormat .
        FILTER(?p = <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)

        ?format rdfs:label ?format_label .
        OPTIONAL { ?format rdfs:comment ?format_comment }

        ?spec cpmeta:hasFormat ?format .
        ?spec rdfs:label ?spec_label .
        OPTIONAL { ?spec rdfs:comment ?spec_comment }

        OPTIONAL { ?spec cpmeta:hasEncoding/rdfs:label ?encoding  }

        FILTER(contains(?format_label, "time"))
        FILTER(!contains(?spec_label, "Meteo"))
        FILTER(!contains(?format_label, "ETC") && !contains(?format_label, "OTC"))

    }
    """
    return query


def parse_binding(b: dict) -> dict:
    """
    Parse the bindings returned when running meta.sparql_select e.g.

    res = meta.sparql_select(query=query)

    The sparql_select search will return a SparqlResults object which contains
    both variable_names (list) and bindings (list of dicts).
    This function expects each `res.bindings` value (dict) so these can be parsed.

    Args:
        b: dictionary containing the binding details from a sparql query.
    Returns:
        dict: Formatted dictionary extracting the column names, values and (where possible) dtypes.
    """
    res = {}
    for k, v in b.items():
        if hasattr(v, "uri"):
            if k == "project_name":
                res[k] = v.uri.split("/")[-1]
            else:
                res[k] = v.uri
        else:
            val = v.value
            if v.datatype is not None and v.datatype.split("#")[-1] in ("float", "double"):
                res[k] = float(val)
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    res[f"{k}_{k2}"] = v2
            else:
                res[k] = val
    return res


def make_query_df(query: str) -> pd.DataFrame:
    """
    Helper function to create a DataFrame from the data returned from the SPARQL search.
    Args:
        query: SPARQL query to use to retrieve data. Full data queries can be created
            using data_query() function.
    Returns:
        pandas.DataFrame: Output from SPARQL query parsed into a DataFrame
    """
    res = meta.sparql_select(query=query)
    return pd.DataFrame([parse_binding(b) for b in res.bindings])


@lru_cache
def icos_format_info() -> pd.DataFrame:
    """
    Extract (and cache) the general ICOS format info, creating a DataFrame for
    unique entries associated with each spec_label.

    Returns:
        pandas.DataFrame: Reference DataFrame for ICOS format info
    """
    format_df = make_query_df(format_query()).drop_duplicates(subset="spec_label")
    format_df = format_df.set_index("spec_label")
    format_df["fmt"] = format_df.format.str.split("/").str[-1]
    return format_df


def dobj_info(
    site: str | list[str] | None = None,
    data_level: int | None = None,
    species: str | list[str] | None = None,
    inlet: str | list[str] | None = None,
    spec_label: str | list[str] | None = None,
    spec_label_include: bool | list[bool] = True,
    filename_str: str | None = None,
    project: str | list[str] | None = ["icos", "euroObspack"],
    format_info: bool = False,
) -> pd.DataFrame:
    """
    Retrieve data object information based on ICOS CP SPARQL search.

    Args:
        site: site or list of sites to search (e.g. "MHD" or ["JFJ", "CMN"]); if
        None, search all sites.
        data_level: ICOS data level: 1, 2 or None for both.
        species: species or list of species to search for.
        inlet: inlet or list of inlets. Only exact matches are found. The inlets
          should be strings, since this is OpenGHG convention, but "100", "100.0",
          "100m" will all work.
        spec_label: regex or regexes to filter the "spec label" by. If an ordinary string
          is passed, only spec labels that contain that string as a substring will
          be matched.
        spec_label_include: For spec_label this includes a regex/string search.
            The spec_label_include input specifies whether the spec_label value or values
            should be included or excluded in the search.
            Examples:
            - spec_label = "CO2", spec_label_include = True
                - search for spec_label which includes "CO2"
            - spec_label = ["CO2", "time"], spec_label_include = True
                - search for spec_label which includes "CO2" AND "time"
            - spec_label = ["CO2", "Obspack"], spec_label_include = [True, False]
                - search for spec_label which includes "CO2" AND excludes "Obspack"
            Default = True
            Only used if spec_label is specified (not None).
        filename_str: regex to filter the "file_name" by. If an ordinary string
            is passed, only filenames that contain that string as a substring will
            be matched.
        project: list of case-sensitive project names as defined here:
            https://meta.icos-cp.eu/ontologies/cpmeta/Project
        format_info: whether to include additional format information (based on icos_format_info()) for
            the data object entrues.
    Returns:
        pandas.DataFrame: SPARQL query search details
    """
    query = data_query(
        site, data_level, species, inlet, spec_label, spec_label_include, filename_str, project
    )
    dobj_df = make_query_df(query)

    if format_info:
        return dobj_df.merge(icos_format_info().reset_index(), on="spec_label", how="left")

    return dobj_df
