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
        include: For spec_label this includes a regex/string search.
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
        if include[i] == False:
            filter_str += "!"

        filter_str += rf'REGEX(?specLabel, "{label}", "i")'

        # If there are still more conditions, include an AND (&&) condition
        if i + 1 < len(spec_label):
            filter_str += " && "
    filter_str += ")"

    return filter_str


def make_site_filter(site: str | list[str] | None) -> str:
    """ """
    if site is None:
        return ""

    site = [site] if isinstance(site, str) else site
    site = [f'"{s.upper()}"' for s in site]
    sites = " ".join(site)
    site_filt = f"VALUES ?stationId {{ {sites} }}"
    return site_filt


def make_data_level_filter(data_level: int | None) -> str:
    """ """
    if data_level is None:
        return ""
    return f"FILTER(?data_level = {data_level})"


def make_species_filter(species: str | list[str] | None) -> str:
    """ """
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
    """ """
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
    """ """
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
        project: list of case-sensitive project names as defined here:
          https://meta.icos-cp.eu/ontologies/cpmeta/Project
        custom_filter: text added directly to the end of the SPARQL query string
        strict: If False, don't try to check that the species found is correct.
          By default, this is True, and we try to check that the inferred species
          is present in some other type of metadata.

    Returns:
        SPARQL query string.

    """
    site_filt = make_site_filter(site)
    data_level_filt = make_data_level_filter(data_level)
    species_filt = make_species_filter(species)
    inlet_filt = make_inlet_filter(inlet)
    spec_filt = make_spec_filter(spec_label, spec_label_include)
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
    This creates a query to extract the following columns:
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
    """ """
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
    """Parse `BindingsList` returned to meta.sparql_select."""
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
    """ """
    res = meta.sparql_select(query=query)
    return pd.DataFrame([parse_binding(b) for b in res.bindings])


@lru_cache
def icos_format_info() -> pd.DataFrame:
    """ """
    format_df = make_query_df(format_query()).drop_duplicates(subset="spec_label")
    format_df = format_df.set_index("spec_label")
    format_df["fmt"] = format_df.format.str.split("/").str[-1]
    return format_df


def dobj_info(
    site: str | list[str] | None = None,
    data_level: int | None = None,
    species: str | list[str] | None = None,
    inlet: str | list[str] | None = None,
    spec_label: str | None = None,
    spec_label_include: bool | list[bool] = True,
    format_info: bool = False,
) -> pd.DataFrame:
    """ """
    query = data_query(site, data_level, species, inlet, spec_label, spec_label_include)
    dobj_df = make_query_df(query)

    if format_info:
        return dobj_df.merge(icos_format_info().reset_index(), on="spec_label", how="left")

    return dobj_df
