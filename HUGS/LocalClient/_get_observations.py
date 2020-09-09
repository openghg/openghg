__all__ = ["get_obs"]

def get_obs(
    sites,
    species,
    start_date=None,
    end_date=None,
    inlet=None,
    average=None,
    keep_missing=False,
    network=None,
    instrument=None,
    status_flag_unflagged=None,
    max_level=None,
    data_directory=None,
    file_paths=None,
    calibration_scale=None,
):
    """ This is the equivalent of the get_obs function from the ACRG repository.

        Usage and return values are the same whilst implementation may differ.

        Args:

    """
    # Search terms should be given as a dictionary



    # Check if we're give a valid site
    # Load in acrg_site_info.json and check site in keys - also do reverse check for longer name?

    # Check species synonyms
    pass


def get_single_site(
    site,
    species,
    network=None,
    start_date=None,
    end_date=None,
    inlet=None,
    average=None,
    instrument=None,
    status_flag_unflagged=[0],
    keep_missing=None,
    file_path=None,
    calibration_scale=None,
    verbose=False,
):
    """
    Get measurements from one site as a list of xarray datasets.
    If there are multiple instruments and inlets at a particular site, 
    note that the acrg_obs_defaults.csv file may be referenced to determine which instrument and inlet to use for each time period.
    If an inlet or instrument changes at some point during time period, multiple datasets will be returned,
    one for each inlet/instrument.

    Args:    
        site_in (str) :
            Site of interest. All sites should be defined within acrg_site_info.json. 
            E.g. ["MHD"] for Mace Head site.
        species_in (str) :
            Species identifier. All species names should be defined within acrg_species_info.json. 
            E.g. "ch4" for methane.
        start_date (str, optional) : 
            Output start date in a format that Pandas can interpret
            Default = None.
        end_date (str, optional) : 
            Output end date in a format that Pandas can interpret
            Default=None.
        inlet (str/list, optional) : 
            Inlet label. If you want to merge all inlets, use "all"
            Default=None
        average (str/list, optional) :
            Averaging period for each dataset (for each site) ((must match number of sites)).
            Each value should be a string of the form e.g. "2H", "30min" (should match pandas offset 
            aliases format).
            Default=None.
        keep_missing (bool, optional) :
            Whether to keep missing data points or drop them.
            default=False.
        network (str/list, optional) : 
            Network for the site/instrument (must match number of sites).
            Default=None.
        instrument (str/list, optional):
            Specific instrument for the site (must match number of sites). 
            Default=None.
        status_flag_unflagged (list, optional) : 
            The value to use when filtering by status_flag. 
            Default = [0]
        calibration_scale (str, optional) :
            Convert to this calibration scale (original scale and new scale must both be in acrg_obs_scale_convert.csv)
    Returns:
        (list of xarray datasets):
            Mole fraction time series data as an xarray dataset, returned in a list. 
            Each list element is for a unique instrument and inlet.
            If either of these changes at some point during the timeseries, they are added as separate list elements.
    """
    from HUGS.Util import load_hugs_json

    # Do a search
    # Get the list of files

    # If we want averaging do some processing

    # Load in site info
    site_info = load_hugs_json(filename="acrg_site_info.json")

    if site not in site_info:
        raise ValueError(f"No site called {site}, please enter a valid site name.")

    # Find the correct synonym for the passed species
    species = synonyms(species)

    # Search for the species at the site at the inlet etc    


def synonyms(species: str) -> str:
    """
    Check to see if there are other names that we should be using for
    a particular input. E.g. If CFC-11 or CFC11 was input, go on to use cfc-11,
    as this is used in species_info.json

    Args:
        species (str): Input string that you're trying to match
    Returns:
        str: Matched species string
    """
    from HUGS.Util import load_hugs_json

    # Load in the species data
    species_data = load_hugs_json(filename="acrg_species_info.json")

    # First test whether site matches keys (case insensitive)
    matched_strings = [k for k in species_data if k.upper() == species.upper()]

    # Used to access the alternative names in species_data
    alt_label = "alt"

    updated_species = None
    # If not found, search synonyms
    if not matched_strings:
        for key in species_data:
            # Iterate over the alternative labels and check for a match
            matched_strings = [s for s in species_data[key][alt_label] if s.upper() == species.upper()]

            if matched_strings:
                updated_species = key
                break

    if updated_species:
        if updated_species != species:
            print(f"Updating species from {species} to {updated_species}")
        return matched_strings
    else:
        raise ValueError(f"Unable to find synonym for species {species}")



