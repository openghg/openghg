from typing import Dict, Optional, List
from xarray import Dataset

__all__ = ["assign_attributes", "get_attributes"]


def assign_attributes(
    data: Dict,
    site: str,
    network: Optional[str] = None,
    sampling_period: Optional[int] = None,
) -> Dict:
    """Assign attributes to the data we've processed. This ensures that the xarray Datasets produced
    as CF 1.7 compliant. Some of the attributes written to the Dataset are saved as metadata
    to the Datasource allowing more detailed searching of data.

    Args:
        data: Dictionary containing data, metadata and attributes
        site: Site code
        sampling_period: Number of seconds for which air
                         sample is taken. Only for time variable attribute
        network: Network name
    Returns:
        dict: Dictionary of combined data with correct attributes assigned to Datasets
    """
    for key in data:
        site_attributes = data[key]["attributes"]
        species = data[key]["metadata"]["species"]

        units = data[key].get("metadata", {}).get("units")
        scale = data[key].get("metadata", {}).get("scale")

        if sampling_period is None:
            sampling_period = data[key].get("metadata", {}).get("sampling_period")

        data[key]["data"] = get_attributes(
            ds=data[key]["data"],
            species=species,
            site=site,
            network=network,
            units=units,
            scale=scale,
            global_attributes=site_attributes,
            sampling_period=sampling_period,
        )

    return data


def get_attributes(
    ds: Dataset,
    species: str,
    site: str,
    network: Optional[str] = None,
    global_attributes: Optional[Dict] = None,
    units: Optional[str] = None,
    scale: Optional[str] = None,
    sampling_period: Optional[int] = None,
    date_range: Optional[List[str]] = None,
) -> Dict:
    """
    This function writes attributes to an xarray.Dataset so that they conform with
    the CF Convention v1.7

    Attributes of the xarray DataSet are modified, and variable names are changed

    If the species is a standard mole fraction then either:
        - species name will used in lower case in the file and variable names
            but with any hyphens taken out
        - name will be changed according to the species_translator dictionary

    If the species is isotopic data or a non-standard variable (e.g. APO):
        - Isotopes species names should begin with a "D"
            (Annoyingly, the code currently picks up "Desflurane" too. I've
             fixed this for now, but if we get a lot of other "D" species, we
             should make this better)
        - I suggest naming for isotopologues should be d<species><isotope>, e.g.
            dCH4C13, or dCO2C14
        - Any non-standard variables should be listed in the species_translator
            dictionary

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
    """
    from pandas import Timestamp as pd_Timestamp
    from openghg.util import compliant_string, load_json

    # from numpy import unique as np_unique

    if not isinstance(ds, Dataset):
        raise TypeError("This function only accepts xarray Datasets")

    # Current CF Conventions (v1.7) demand that valid variable names
    # begin with a letter and be composed of letters, digits and underscores
    # Here variable names are also made lowercase to enable easier matching below
    to_underscores = {var: var.lower().replace(" ", "_") for var in ds.variables}
    ds = ds.rename(to_underscores)

    species_attrs = load_json(filename="species_attributes.json")
    attributes_data = load_json("attributes.json")

    species_translator = attributes_data["species_translation"]
    unit_species = attributes_data["unit_species"]
    unit_species_long = attributes_data["unit_species_long"]
    unit_interpret = attributes_data["unit_interpret"]

    species_upper = species.upper()
    species_lower = species.lower()

    matched_keys = [var for var in ds.variables if species_lower in var]

    # If we don't have any variables to rename, raise an error
    if not matched_keys:
        raise NameError(f"Cannot find species {species} in Dataset variables")

    species_rename = {}
    for var in matched_keys:
        try:
            species_label = species_translator[species_upper]["chem"]
        except KeyError:
            species_label = compliant_string(species_lower)

        species_rename[var] = var.replace(species_lower, species_label)

    ds = ds.rename(species_rename)

    # Global attributes
    global_attributes_default = {
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
    }

    if global_attributes:
        global_attributes.update(global_attributes_default)
    else:
        global_attributes = global_attributes_default

    global_attributes["File created"] = str(pd_Timestamp.now(tz="UTC"))
    global_attributes["Processed by"] = "OpenGHG_Cloud"
    global_attributes["species"] = species_label

    if scale is None:
        global_attributes["Calibration_scale"] = "unknown"
    else:
        global_attributes["Calibration_scale"] = scale

    # Update the Dataset attributes
    ds.attrs.update(global_attributes)

    # Add some site attributes
    site_attributes = _site_info_attributes(site.upper(), network)
    ds.attrs.update(site_attributes)

    # Species-specific attributes
    # Long name
    if species_upper.startswith("D") and species_upper != "DESFLURANE" or species_upper == "APD":
        sp_long = species_translator[species_upper]["name"]
    elif species_upper == "RN":
        sp_long = "radioactivity_concentration_of_222Rn_in_air"
    elif species_upper in species_translator:
        name = species_translator[species_upper]["name"]
        sp_long = f"mole_fraction_of_{name}_in_air"
    else:
        sp_long = f"mole_fraction_of_{species_label}_in_air"

    ancillary_variables = []

    matched_keys = [var for var in ds.variables if species_lower in var.lower()]

    # Write units as attributes to variables containing any of these
    match_words = ["variability", "repeatability", "stdev", "count"]

    for key in ds.variables:
        key = key.lower()

        if species_label.lower() in key:
            # Standard name attribute
            # ds[key].attrs["standard_name"]=key.replace(species_label, sp_long)
            ds[key].attrs["long_name"] = key.replace(species_label, sp_long)

            # If units are required for variable, add attribute
            if key == species_label or any(word in key for word in match_words):
                if units is not None:
                    if units in unit_interpret:
                        ds[key].attrs["units"] = unit_interpret[units]
                    else:
                        ds[key].attrs["units"] = unit_interpret["else"]
                else:
                    # TODO - merge these species attributes into a single simpler JSON
                    try:
                        ds[key].attrs["units"] = unit_species[species_upper]
                    except KeyError:
                        try:
                            ds[key].attrs["units"] = species_attrs[species_label.upper()]["units"]
                        except KeyError:
                            ds[key].attrs["units"] = "NA"

                # If units are non-standard, add explanation
                if species_upper in unit_species_long:
                    ds[key].attrs["units_description"] = unit_species_long[species_upper]

            # Add to list of ancilliary variables
            if key != species_label:
                ancillary_variables.append(key)

    # TODO - for the moment skip this step - check status of ancilliary variables in standard
    # Write ancilliary variable list
    # ds[species_label].attrs["ancilliary_variables"] = ", ".join(ancillary_variables)

    # Add quality flag attributes
    # NOTE - I've removed the whitespace before status_flag and integration_flag here
    quality_flags = [key for key in ds.variables if "status_flag" in key]

    # Not getting long_name for c2f6

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

    # Add integration flag attributes
    integration_flags = [key for key in ds.variables if "integration_flag" in key]

    for key in integration_flags:
        ds[key] = ds[key].astype(int)
        long_name = ds[species_label].attrs["long_name"]
        ds[key].attrs = {
            "flag_meaning": "0 = area, 1 = height",
            "standard_name": f"{long_name} integration_flag",
            "comment": "GC peak integration method (by height or by area). Does not indicate data quality",
        }

    # Set time encoding
    # Check if there are duplicate time stamps

    # I feel there should be a more pandas way of doing this
    # but xarray doesn't currently have a duplicates method
    # See this https://github.com/pydata/xarray/issues/2108

    # if len(set(ds.time.values)) < len(ds.time.values):
    # if len(np_unique(ds.time.values)) < len(ds.time.values):
    #     print("WARNING. Duplicate time stamps")

    first_year = pd_Timestamp(ds.time[0].values).year

    ds.time.encoding = {"units": f"seconds since {str(first_year)}-01-01 00:00:00"}

    time_attributes = {}
    time_attributes["label"] = "left"
    time_attributes["standard_name"] = "time"
    time_attributes["comment"] = (
        "Time stamp corresponds to beginning of sampling period. "
        + "Time since midnight UTC of reference date. "
        + "Note that sampling periods are approximate."
    )

    if sampling_period is not None:
        time_attributes["sampling_period_seconds"] = sampling_period

    ds.time.attrs.update(time_attributes)

    # If a date range is specified, slice dataset
    if date_range:
        ds = ds.loc[dict(time=slice(*date_range))]

    return ds


def _site_info_attributes(site: str, network: Optional[str] = None) -> Dict:
    """Reads site attributes from JSON

    Args:
        site: Site code
        network: Network name
    Returns:
        dict: Dictionary of site attributes
    """
    from openghg.util import load_json

    site = site.upper()

    # Read site info file
    data_filename = "acrg_site_info.json"
    site_params = load_json(filename=data_filename)

    if network is None:
        network = list(site_params[site].keys())[0]
    else:
        network = network.upper()

    attributes_dict = {
        "longitude": "station_longitude",
        "latitude": "station_latitude",
        "long_name": "station_long_name",
        "height_station_masl": "station_height_masl",
    }

    attributes = {}
    if site in site_params:
        for attr in attributes_dict:
            if attr in site_params[site][network]:
                attr_key = attributes_dict[attr]

                attributes[attr_key] = site_params[site][network][attr]
    else:
        raise ValueError(f"Invalid site {site} passed. Please use a valid site code such as BSD for Bilsdale")

    return attributes
