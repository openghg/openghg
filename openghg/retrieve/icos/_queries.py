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

from collections import defaultdict
import re

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


def make_spec_filter(spec_label: str | None = None) -> str:
    if spec_label is None:
        return ""
    return f'FILTER REGEX (?specLabel, {spec_label}, "i")'


def make_site_filter(site: str | list[str] | None) -> str:
    if site is None:
        return ""

    site = [site] if isinstance(site, str) else site
    site = [f'"{s.upper()}"' for s in site]
    sites = " ".join(site)
    site_filt = f"VALUES ?stationId {{ {sites} }}"
    return site_filt


def make_data_level_filter(data_level: int | None) -> str:
    if data_level is None:
        return ""
    return f"FILTER(?data_level = {data_level})"


def make_species_filter(species: str | list[str] | None) -> str:
    if species is None:
        return ""

    if isinstance(species, str):
        species = [species]
    species = [s.lower() for s in species]

    # sparql requires double quotes for strings, so we can't just use f"... {tuple(species)}"
    formatted_tuple = "(" + ", ".join(f'"{s}"' for s in species) + ")"
    species_filt = f"""
    # filter species
    FILTER(LCASE(?varName) in {formatted_tuple})
    """
    return species_filt


def make_inlet_filter(inlet: str | list[str] | None) -> str:
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
    spec_label: str | None = None,
) -> str:
    site_filt = make_site_filter(site)
    data_level_filt = make_data_level_filter(data_level)
    species_filt = make_species_filter(species)
    inlet_filt = make_inlet_filter(inlet)
    spec_filter = make_spec_filter(spec_label)

    query = sparql_header()
    query += "SELECT (?stationId as ?site) (?samplingHeight as ?inlet) ?species ?data_level ?file_name (?specLabel as ?spec_label) (?dobj as ?dobj_uri)\n"

    query += f"""
    WHERE {{
        # filter to select only data without a previous version
        FILTER NOT EXISTS {{[] cpmeta:isNextVersionOf ?dobj}}

        # get station ID (e.g. TAC, JFJ, etc)
        ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
        ?station cpmeta:hasStationId ?stationId .  # ICOS doesn't like if this is chained with the previous query
        {site_filt}

        # get object spec
        ?dobj cpmeta:hasObjectSpec ?spec .

        ?spec cpmeta:hasDataLevel ?data_level .
        {data_level_filt}

        # spec label
        ?spec rdfs:label ?specLabel .
        {spec_filter}

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
        {species_filt}

        # filter to link dobj and varName
        # netCDF data doesn't seem to have the ?colNames variable, so we
        # need to detect it by checking if ?varName = "value"
        OPTIONAL {{ ?dobj cpmeta:hasActualColumnNames ?colNames . }}
        FILTER(
            COALESCE(CONTAINS(?colNames, concat('"', ?varName, '"')), FALSE)
            || (!BOUND(?colNames) && (?varName = "value"))
        )

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
    }}
    """
    return query


def attrs_query(dobj_uri: str) -> str:
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
            ?dobj cpmeta:hasActualColumnNames ?colNames .
            FILTER(CONTAINS(?colNames, CONCAT('"', ?col_label, '"')))
    }}
    """
    return query


def format_query() -> str:
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


def parse_binding(b):
    """Parse `BindingsList` returned to meta.sparql_select."""
    res = {}
    for k, v in b.items():
        if hasattr(v, "uri"):
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


def make_query_df(query):
    res = meta.sparql_select(query=query)
    return pd.DataFrame([parse_binding(b) for b in res.bindings])


def icos_format_info() -> pd.DataFrame:
    return make_query_df(format_query()).drop_duplicates(subset="spec_label")


def dobj_info(
    site: str | list[str] | None = None,
    data_level: int | None = None,
    species: str | list[str] | None = None,
    inlet: str | list[str] | None = None,
    spec_label: str | None = None,
    format_info: bool = False,
) -> pd.DataFrame:
    query = data_query(site, data_level, species, inlet, spec_label)
    dobj_df = make_query_df(query)

    if format_info:
        return dobj_df.merge(icos_format_info(), on="spec_label", how="left")

    return dobj_df
