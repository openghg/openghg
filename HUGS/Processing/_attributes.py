__all__ = ["get_attributes"]

def attributes(ds, species, site,
               network = None,
               global_attributes = None,
               units = None,
               scale = None,
               sampling_period = None,
               date_range = None,
               global_attributes_default=None):
    """
    Format attributes for netCDF file
    Attributes of xarray DataSet are modified, and variable names are changed

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
        ds (xarray dataset): Should contain variables such as "ch4", "ch4 repeatability".
            Must have a "time" dimension.
        species (string): Species name. e.g. "CH4", "HFC-134a", "dCH4C13"
        site (string): Three-letter site code

        global_attribuates (dict, optional): Dictionary containing any info you want to
            add to the file header (e.g. {"Contact": "Matt Rigby"})
        units (string, optional): This routine will try to guess the units
            unless this is specified. Options are in units_interpret
        scale (string, optional): Calibration scale for file header.
        sampling_period (int, optional): Number of seconds for which air
            sample is taken. Only for time variable attribute
        date_range (list of two strings, optional): Start and end date for output
            If you only want an end date, just put a very early start date
            (e.g. ["1900-01-01", "2010-01-01"])
    """
    from pathlib import Path
    # Rename all columns to lower case! Could this cause problems?
    rename_dict = {var: var.lower() for var in ds.variables}
    ds = ds.rename(rename_dict)

    data_path = Path(__file__).resolve().parent.joinpath("../Data/attributes.json")
    with open(data_path, "r") as f:
            data = json.load(f)
            species_translator = data["species_translation"]

    # # Rename species, if required
    # rename_dict = {}
    # for key in ds.variables:
    #     if species.lower() in key:
    #         if species.upper() in list(species_translator.keys()):
    #             # Rename based on species_translator, if available
    #             species_out = species_translator[species.upper()][0]
    #         else:
    #             # Rename species to be lower case and without hyphens
    #             species_out = species.lower().replace("-", "")

    #         rename_dict[key] = key.replace(species.lower(), species_out)

    species_upper = species.upper()
    species_lower = species.lower()

    to_rename = [var for var in ds.variables if species_lower in var]

    # If we don't have any variables to rename, raise an error
    if not to_rename:
        raise NameError(f"Cannot find speces {species} in Dataset variables")
    
    rename_dict = {}
    for var in to_rename:
        if species_upper in species_translator:
            new_label = species_translator[species_upper]["chem"]
        else:
            new_label = species_lower.replace("-", "")
        
        rename_dict[var] = var.replace(species_lower, new_label)

    ds = ds.rename(rename_dict)


    # Global attributes
    #############################################

    if global_attributes_default is None:
        global_attributes_default =  {"Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
                                       "Source": "In situ measurements of air",
                                       "Conventions": "CF-1.6"}

    if global_attributes is None:
        global_attributes = {}
        for key in global_attributes_default:
            global_attributes[key] = global_attributes_default[key]
    else:
        for key in global_attributes_default:
            global_attributes[key] = global_attributes_default[key]

    # Add some defaults
    global_attributes["File created"] = str(dt.now())

    # Add user
    global_attributes["Processed by"] = "%s@bristol.ac.uk" % getpass.getuser()


    for key, values in global_attributes.items():
        ds.attrs[key] = values

    # Add some site attributes
    global_attributes_site = site_info_attributes(site.upper(), network)
    if global_attributes_site is not None:
        for key, values in global_attributes_site.items():
            ds.attrs[key] = values

    # Add calibration scale
    if scale:
        ds.attrs["Calibration_scale"] = scale
    else:
        ds.attrs["Calibration_scale"] = "unknown"

    # Add species name
    ds.attrs["species"] = species_out

    # Species-specific attributes
    #############################################

    # Long name
    if (species.upper()[0] == "D" and species.upper() != "DESFLURANE") or species.upper() == "APO":
        sp_long = species_translator[species.upper()][1]
    elif species.upper() == "RN":
        sp_long = "radioactivity_concentration_of_222Rn_in_air"
    elif species.upper() in list(species_translator.keys()):
        sp_long = "mole_fraction_of_" + species_translator[species.upper()][1] + "_in_air"
    else:
        sp_long = "mole_fraction_of_" + species_out + "_in_air"

    ancillary_variables = ""

    for key in ds.variables:

        if species_out in key:

            # Standard name attribute
            #ds[key].attrs["standard_name"]=key.replace(species_out, sp_long)
            ds[key].attrs["long_name"]=key.replace(species_out, sp_long)

            # If units are required for variable, add attribute
            if (key == species_out) or \
                ("variability" in key) or \
                ("repeatability" in key):
                if units is None:
                    ds[key].attrs["units"] = unit_species[species.upper()]
                else:
                    if units in list(unit_interpret.keys()):
                        ds[key].attrs["units"] = unit_interpret[units]
                    else:
                        ds[key].attrs["units"] = unit_interpret["else"]

                # if units are non-standard, add explanation
                if species.upper() in list(unit_species_long.keys()):
                    ds[key].attrs["units_description"] = unit_species_long[species.upper()]

            # Add to list of ancilliary variables
            if key != species_out:
                ancillary_variables += key + ", "

    # Write ancilliary variable list
    ds[species_out].attrs["ancilliary_variables"] = ancillary_variables.strip()

    # Add quality flag attributes
    ##################################

    flag_key = [key for key in ds.variables if " status_flag" in key]
    if len(flag_key) > 0:
        flag_key = flag_key[0]
        ds[flag_key] = ds[flag_key].astype(int)
        ds[flag_key].attrs = {"flag_meaning":
                              "0 = unflagged, 1 = flagged",
                              "long_name":
                              ds[species_out].attrs["long_name"] + " status_flag"}

    # Add integration flag attributes
    ##################################

    flag_key = [key for key in ds.variables if " integration_flag" in key]
    if len(flag_key) > 0:
        flag_key = flag_key[0]
        ds[flag_key] = ds[flag_key].astype(int)
        ds[flag_key].attrs = {"flag_meaning":
                              "0 = area, 1 = height",
                              "standard_name":
                              ds[species_out].attrs["long_name"] + " integration_flag",
                              "comment":
                              "GC peak integration method (by height or by area). " +
                              "Does not indicate data quality"}

    # Set time encoding
    #########################################

    # Check if there are duplicate time stamps
    if len(set(ds.time.values)) < len(ds.time.values):
        print("WARNING. Dupliate time stamps")

    first_year = str(ds.time.to_pandas().index.to_pydatetime()[0].year)

    ds.time.encoding = {"units": "seconds since " + \
                        first_year + "-01-01 00:00:00"}
    ds.time.attrs["label"] = "left"
    ds.time.attrs["comment"] = "Time stamp corresponds to beginning of sampling period. " + \
                               "Time since midnight UTC of reference date. " + \
                               "Note that sampling periods are approximate."
    if sampling_period:
        ds.time.attrs["sampling_period_seconds"] = sampling_period

    # If a date range is specified, slice dataset
    if date_range != None:
        ds = ds.loc[dict(time = slice(*date_range))]

    return ds
