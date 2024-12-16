from typing import Any, cast
from collections.abc import Hashable
import logging
from xarray import Dataset
from openghg.types import optionalPathType

__all__ = [
    "assign_attributes",
    "get_attributes",
    "define_species_label",
    "assign_flux_attributes",
    "get_flux_attributes",
    "dataset_formatter",
    "data_variable_formatter",
]

logger = logging.getLogger("openghg.standardise")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def assign_attributes(
    data: dict,
    site: str | None = None,
    network: str | None = None,
    sampling_period: str | float | int | None = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    species_filepath: optionalPathType = None,
) -> dict:
    """Assign attributes to each site and species dataset. This ensures that the xarray Datasets produced
    are CF 1.7 compliant. Some of the attributes written to the Dataset are saved as metadata
    to the Datasource allowing more detailed searching of data.

    If accessing underlying stored site or species definitions, this will
    be accessed from the openghg/openghg_defs repository by default.

    Args:
        data: Dictionary containing data, metadata and attributes
        site: Site code
        sampling_period: Number of seconds for which air
                         sample is taken. Only for time variable attribute
        network: Network name
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file
        species_filepath: Alternative species info file

    Returns:
        dict: Dictionary of combined data with correct attributes assigned to Datasets
    """

    for _, gas_data in data.items():
        site_attributes = gas_data.get("attributes", {})
        species = gas_data["metadata"]["species"]

        if site is None:
            site = gas_data.get("metadata", {}).get("site")
        if network is None:
            network = gas_data.get("metadata", {}).get("network")

        units = gas_data.get("metadata", {}).get("units")
        scale = gas_data.get("metadata", {}).get("calibration_scale")

        if sampling_period is None:
            sampling_period = str(gas_data.get("metadata", {}).get("sampling_period", "NOT_SET"))

        gas_data["data"] = get_attributes(
            ds=gas_data["data"],
            species=species,
            site=site,
            network=network,
            units=units,
            scale=scale,
            global_attributes=site_attributes,
            sampling_period=sampling_period,
            site_filepath=site_filepath,
            species_filepath=species_filepath,
        )

    return data


def get_attributes(
    ds: Dataset,
    species: str,
    site: str,
    network: str | None = None,
    global_attributes: dict[str, str] | None = None,
    units: str | None = None,
    scale: str | None = None,
    sampling_period: str | float | int | None = None,
    date_range: list[str] | None = None,
    site_filepath: optionalPathType = None,
    species_filepath: optionalPathType = None,
) -> Dataset:
    """
    This function writes attributes to an xarray.Dataset so that they conform with
    the CF Convention v1.6

    Attributes of the xarray DataSet are modified, and variable names are changed

    If accessing underlying stored site or species definitions, this will
    be accessed from the openghg/openghg_defs repository by default.

    Variable naming related to species name will be defined using
    define_species_label() function.

    Args:
        ds: Should contain variables such as "ch4", "ch4 repeatability".
            Must have a "time" dimension.
        species: Species name. e.g. "CH4", "HFC-134a", "dCH4C13"
        site: Three-letter site code
        network: Network site is associated with
        global_attribuates: Dictionary containing any info you want to
            add to the file header (e.g. {"Contact": "Contact_Name"})
        units: This routine will try to guess the units
            unless this is specified. Options are in units_interpret
        scale: Calibration scale for species.
        sampling_period: Number of seconds for which air
            sample is taken. Only for time variable attribute
        date_range: Start and end date for output
            If you only want an end date, just put a very early start date
            (e.g. ["1900-01-01", "2010-01-01"])
        site_filepath: Alternative site info file
        species_filepath: Alternative species info file
    """
    from openghg.util import load_internal_json, timestamp_now, get_species_info
    from pandas import Timestamp as pd_Timestamp

    if not isinstance(ds, Dataset):
        raise TypeError("This function only accepts xarray Datasets")

    # Load attributes files
    species_attrs = get_species_info()
    attributes_data = load_internal_json(filename="attributes.json")

    unit_interpret = attributes_data["unit_interpret"]
    unit_mol_fraction = attributes_data["unit_mol_fraction"]
    unit_non_standard = attributes_data["unit_non_standard"]

    # Extract both label to use for species and key for attributes
    # Typically species_label will be the lower case version of species_key
    species_label, species_key = define_species_label(species, species_filepath)

    # Global attributes
    global_attributes_default = {
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "source": "In situ measurements of air",
        "Conventions": "CF-1.8",
    }

    if global_attributes is not None:
        # TODO - for some reason mypy doesn't see a Dict[str,str] as a valid Mapping[Hashable, Any] type
        global_attributes.update(global_attributes_default)  # type: ignore
    else:
        global_attributes = global_attributes_default

    global_attributes["file_created"] = str(timestamp_now())
    global_attributes["processed_by"] = "OpenGHG_Cloud"
    global_attributes["species"] = species_label

    if scale is None:
        global_attributes["calibration_scale"] = "unknown"
    else:
        global_attributes["calibration_scale"] = scale

    if sampling_period is None:
        global_attributes["sampling_period"] = "NOT_SET"
    else:
        global_attributes["sampling_period"] = str(sampling_period)
        global_attributes["sampling_period_unit"] = "s"

    # 04/2023: Switched around global and site attributes so
    # global attributes now supercede site attributes.
    # Add some site attributes
    site_attributes = _site_info_attributes(site.upper(), network, site_filepath)
    ds.attrs.update(site_attributes)

    # Update the Dataset attributes
    ds.attrs.update(global_attributes)  # type: ignore

    # Species-specific attributes
    # Extract long name
    try:
        name = species_attrs[species_key]["name"]
    except KeyError:
        name = species_label

    # Extract units if not defined
    if units is None:
        try:
            units = species_attrs[species_key]["units"]
        except KeyError:
            units = ""

    # Define label based on units
    if units in unit_mol_fraction:
        sp_long = f"mole_fraction_of_{name}_in_air"
    else:
        sp_long = name

    ancillary_variables = []

    variable_names = cast(dict[str, Any], ds.variables)

    # Write units as attributes to variables containing any of these
    match_words = ["variability", "repeatability", "stdev", "count"]

    for key in variable_names:
        key = key.lower()

        if species_label in key:
            # Standard name attribute
            # ds[key].attrs["standard_name"]=key.replace(species_label, sp_long)
            ds[key].attrs["long_name"] = key.replace(species_label, sp_long)

            # If units are required for variable, add attribute
            if key == species_label or any(word in key for word in match_words):
                if units in unit_interpret:
                    ds[key].attrs["units"] = unit_interpret[units]
                    # If units are non-standard, add details
                    if units in unit_non_standard:
                        ds[key].attrs["units_description"] = units
                elif units == "":
                    ds[key].attrs["units"] = unit_interpret["else"]
                else:
                    ds[key].attrs["units"] = units

            # Add to list of ancilliary variables
            if key != species_label:
                ancillary_variables.append(key)

    # TODO - for the moment skip this step - check status of ancilliary variables in standard
    # Write ancilliary variable list
    # ds[species_label_lower].attrs["ancilliary_variables"] = ", ".join(ancillary_variables)

    # Add quality flag attributes
    # NOTE - I've removed the whitespace before status_flag and integration_flag here
    variable_names = cast(dict[str, Any], ds.variables)
    quality_flags = [key for key in variable_names if "status_flag" in key]

    for key in quality_flags:
        ds[key] = ds[key].astype(int)
        try:
            long_name = ds[species_label].attrs["long_name"]
        except KeyError:
            raise KeyError(key, quality_flags)

        ds[key].attrs = {
            "flag_meaning": "0 = unflagged, 1 = flagged",
            "long_name": f"{long_name} status_flag",
        }

    variable_names = cast(dict[str, Any], ds.variables)
    # Add integration flag attributes
    integration_flags = [key for key in variable_names if "integration_flag" in key]

    for key in integration_flags:
        ds[key] = ds[key].astype(int)
        long_name = ds[species_label].attrs["long_name"]
        ds[key].attrs = {
            "flag_meaning": "0 = area, 1 = height",
            "standard_name": f"{long_name} integration_flag",
            "comment": "GC peak integration method (by height or by area). Does not indicate data quality",
        }

    first_year = pd_Timestamp(str(ds.time[0].values)).year

    ds.time.encoding = {"units": f"seconds since {str(first_year)}-01-01 00:00:00"}

    time_attributes: dict[str, str] = {}
    time_attributes["label"] = "left"
    time_attributes["standard_name"] = "time"
    time_attributes["comment"] = (
        "Time stamp corresponds to beginning of sampling period. "
        + "Time since midnight UTC of reference date. "
        + "Note that sampling periods are approximate."
    )

    if sampling_period is not None:
        time_attributes["sampling_period_seconds"] = str(sampling_period)

    ds.time.attrs.update(time_attributes)

    # If a date range is specified, slice dataset
    if date_range:
        ds = ds.loc[dict(time=slice(*date_range))]

    return ds


def define_species_label(species: str, species_filepath: optionalPathType = None) -> tuple[str, str]:
    """Define standardised label to use for observation datasets.
    This uses the data stored within openghg_defs/data/site_info JSON file
    by default with alternative names ('alt') defined within.

    Formatting:
     - species label will be all lower case
     - any spaces will be replaced with underscores
     - if species or synonym cannot be found, species name will used
        but with any hyphens taken out (see also openghg.util.clean_string function)

    Note: Suggested naming for isotopologues should be d<species><isotope>, e.g.
    dCH4C13, or dCO2C14

    Args:
        species : Species name.
        species_filepath : Alternative species info file.
    Returns:
        str, str: Both the species label to be used exactly and the original attribute
                  key needed to extract additional data from the 'site_info.json'
                  attributes file.
    Example:
        >>> define_species_label("methane")
            ("ch4", "CH4")
        >>> define_species_label("radon")
            ("rn", "Rn")
        >>> define_species_label("cfc-11")
            ("cfc11", "CFC11")
        >>> define_species_label("CH4C13")
            ("dch4c13", "DCH4C13")
    """
    from openghg.util import clean_string, synonyms

    # Extract species label using synonyms function
    try:
        species_label = synonyms(
            species, lower=False, allow_new_species=False, species_filepath=species_filepath
        )
    except ValueError:
        species_underscore = species.replace(" ", "_")
        species_remove_dash = species_underscore.replace("-", "")
        species_label = clean_string(species_remove_dash)

    species_label_lower = species_label.lower()

    return species_label_lower, species_label


def _site_info_attributes(
    site: str, network: str | None = None, site_filepath: optionalPathType = None
) -> dict:
    """Reads site attributes from JSON

    This uses the data stored within openghg_defs/data/site_info JSON file by default.

    Args:
        site: Site code
        network: Network name
        site_filepath: Alternative site info file
    Returns:
        dict: Dictionary of site attributes
    """
    from openghg.util import get_site_info

    site = site.upper()

    # Read site info file
    site_data = get_site_info(site_filepath)

    if network is None:
        network = next(iter(site_data[site]))
    else:
        network = network.upper()

    attributes_dict = {
        "longitude": "station_longitude",
        "latitude": "station_latitude",
        "long_name": "station_long_name",
        "height_station_masl": "station_height_masl",
    }

    attributes = {}
    if site in site_data:
        for attr in attributes_dict:
            try:
                if attr in site_data[site][network]:
                    attr_key = attributes_dict[attr]

                    attributes[attr_key] = site_data[site][network][attr]
            except KeyError:
                pass
    else:
        logger.info(
            f"We haven't seen site {site} before, please let us know so we can update our records."
            + "\nYou can help us by opening an issue on GitHub for our supplementary data: https://github.com/openghg/openghg_defs"
        )
        # TODO - log not seen site message here
        # raise ValueError(f"Invalid site {site} passed. Please use a valid site code such as BSD for Bilsdale")

    return attributes


def assign_flux_attributes(
    data: dict,
    species: str | None = None,
    source: str | None = None,
    domain: str | None = None,
    units: str = "mol/m2/s",
    prior_info_dict: dict | None = None,
) -> dict:
    """
    Assign attributes for the input flux dataset within dictionary based on
    metadata and passed arguments.

    Args:
        data: Dictionary containing data, metadata and attributes
        species: Species name
        source: Source name
        domain: Domain name
        units: Unit values for the "flux" variable.  Default = "mol/m2/s"
        prior_info_dict: Dictionary of additional 'prior' information about
            for the emissions sources. Expect this to be of the form e.g.
                {"EDGAR": {"version": "v4.3.2",
                           "raw_resolution": "0.1 degree x 0.1 degree",
                           "reference": "http://edgar.jrc.ec.europa.eu/overview.php?v=432_GHG"
                           ...},
                ...}

    Returns:
        Dict : Same format as inputted but with updated "data" component (Dataset)
    """

    for flux_dict in data.values():
        flux_attributes = flux_dict.get("attributes", {})

        # Ensure values for these attributes have been specified either manually
        # or within metadata.
        attribute_values = {"species": species, "source": source, "domain": domain}

        metadata = flux_dict["metadata"]
        for attr, value in attribute_values.items():
            if value is None:
                try:
                    attribute_values[attr] = metadata[attr]
                except KeyError:
                    raise ValueError(f"Attribute {attr} must be specified.")

        input_attributes = cast(dict[str, str], attribute_values)

        flux_dict["data"] = get_flux_attributes(
            ds=flux_dict["data"],
            units=units,
            prior_info_dict=prior_info_dict,
            global_attributes=flux_attributes,
            **input_attributes,
        )

    return data


def get_flux_attributes(
    ds: Dataset,
    species: str,
    source: str,
    domain: str,
    units: str = "mol/m2/s",
    prior_info_dict: dict | None = None,
    global_attributes: dict[Hashable, Any] | None = None,
) -> Dataset:
    """
    Assign additional attributes for the flux dataset.

    Args:
        ds: Should contain "flux" variable
        species: Species name
        source: Source name
        domain: Domain name
        units: Unit values for the "flux" variable. Default = "mol/m2/s"
        prior_info_dict: Dictionary of additional 'prior' information about
            for the emissions sources. Expect this to be of the form e.g.
                {"EDGAR": {"version": "v4.3.2",
                           "raw_resolution": "0.1 degree x 0.1 degree",
                           "reference": "http://edgar.jrc.ec.europa.eu/overview.php?v=432_GHG"
                           ...},
                ...}
        global_attributes: Additional global attributes to write to dataset.

    Returns:
        Dataset: Input dataset with updated variable/coordinate and global attributes
    """

    # Example flux attributes (from files)
    # :title = "EDGAR 4.3.2 year 2004" ;
    # :author = "ag12733" ;
    # :date_created = "2018-07-16 13:10:57.346915" ;
    # :number_of_prior_files_used = 1L ;
    # :prior_file_1 = "EDGAR" ;
    # :prior_file_1_version = "/data/shared/Gridded_fluxes/N2O/EDGAR_v4.3.2/v432_N2O_TOTALS_nc/v432_N2O_2004.0.1x0.1.nc" ;
    # :prior_file_1_raw_resolution = "0.1 degree x 0.1 degree" ;
    # :prior_file_1_reference = "http://edgar.jrc.ec.europa.eu/overview.php?v=432_GHG" ;
    # :regridder_used = "acrg_grid.regrid.regrid_3D" ;

    from openghg.util import timestamp_now

    # Define species variable/coordinate attributes and assign
    flux_attrs = {"source": source, "units": units, "species": species}

    lat_attrs = {"long_name": "latitude", "units": "degrees_north", "notes": "centre of cell"}

    lon_attrs = {"long_name": "longitude", "units": "degrees_east", "notes": "centre of cell"}

    ds["flux"].attrs = flux_attrs
    ds["lat"].attrs = lat_attrs
    ds["lon"].attrs = lon_attrs

    # Define default values for global attributes
    global_attributes_default: dict[Hashable, Any] = {
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "Conventions": "CF-1.8",
    }

    if global_attributes is None:
        global_attributes = global_attributes_default
    else:
        global_attributes.update(global_attributes_default)

    # Extract any current attributes from the Dataset
    current_attributes = ds.attrs

    # Extract "title" from current attributes or define this.
    if "title" in current_attributes and "title" not in global_attributes:
        global_attributes["title"] = current_attributes["title"]
    else:
        global_attributes["title"] = f"{source} emissions/flux of {species} for {domain} domain"

    if "file_created" not in global_attributes:
        global_attributes["file_created"] = str(timestamp_now())
    if "process_by" not in global_attributes:
        global_attributes["processed_by"] = "OpenGHG_Cloud"

    species_label = define_species_label(species)

    global_attributes["species"] = species_label
    global_attributes["source"] = source
    global_attributes["domain"] = domain

    # Add any 'prior' information for flux / emissions databases.
    if prior_info_dict is not None:
        # For composite flux / emissions files this may contain > 1 prior input
        global_attributes["number_of_prior_files_used"] = len(prior_info_dict.keys())
        for i, source_key in enumerate(prior_info_dict.keys()):
            prior_number = i + 1
            label_start = f"prior_file_{prior_number}"
            global_attributes[label_start] = source_key

            for key, value in prior_info_dict[source_key].items():
                attr_key = f"{label_start}_{key}"
                global_attributes[attr_key] = value

    # Ensure keys which have been updated by OpenGHG are not overwritten
    # by current attributes.
    updated_keys = ["Conventions", "title", "file_created", "processed_by"]
    for key in updated_keys:
        if key in current_attributes:
            current_attributes.pop(key)

    global_attributes.update(current_attributes)
    ds.attrs = global_attributes

    return ds


def dataset_formatter(
    data: dict,
) -> dict:
    """
    Formats species/variables from the dataset by removing the whitespaces
    with underscores and species to lower case

    Args:
        data: Dict containing dataset information(gas_data)

    Returns:
        Dict: Dictionary of source_name : data, metadata, attributes
    """
    for _, gas_data in data.items():
        species = gas_data["metadata"]["species"]
        species_label, species_key = define_species_label(species)
        gas_data["data"] = data_variable_formatter(
            ds=gas_data["data"], species=species, species_label=species_label
        )

    return data


def data_variable_formatter(ds: Dataset, species: str, species_label: str) -> Dataset:
    """
    Formats variables from the dataset by removing the whitespaces
    with underscores and species data var to lower case

    Args:
        ds: Should contain variables such as "ch4", "ch4 repeatability".
            Must have a "time" dimension.
        species: Species name
        species_label: Species label

    Returns:
        ds: xarray dataset
    """
    variable_names = cast(dict[str, Any], ds.variables)
    to_underscores = {var: var.lower().replace(" ", "_") for var in variable_names}
    to_underscores.pop("time")  # Added to remove warning around resetting time index.
    ds = ds.rename(to_underscores)  # type: ignore

    species_lower = species.lower()
    species_search = species_lower.replace(" ", "_")

    variable_names = cast(dict[str, Any], ds.variables)
    matched_keys = [var for var in variable_names if species_search in var]

    # If we don't have any variables to rename, raise an error
    if not matched_keys:
        raise NameError(f"Cannot find species {species_search} in Dataset variables")

    species_rename = {}
    for var in matched_keys:
        species_rename[var] = var.replace(species_search, species_label)

    ds = ds.rename(species_rename)

    return ds
