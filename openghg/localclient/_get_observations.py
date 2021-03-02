__all__ = ["get_single_site", "scale_convert"]

from xarray import Dataset
from typing import Dict, List, Optional, Union
from pandas import Timestamp
from dataclasses import dataclass
from openghg.localclient import ObsData


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
) -> list:
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
    site: str,
    species: str,
    network: Optional[str] = None,
    start_date: Optional[Union[str, Timestamp]] = None,
    end_date: Optional[Union[str, Timestamp]] = None,
    inlet: Optional[str] = None,
    average: Optional[str] = None,
    instrument: Optional[str] = None,
    keep_missing: Optional[bool] = False,
    calibration_scale: Optional[str] = None,
) -> List[ObsData]:
    """ Get measurements from one site as a list of xarray datasets.
        If there are multiple instruments and inlets at a particular site, 
        note that the acrg_obs_defaults.csv file may be referenced to determine which instrument and inlet to use for each time period.
        If an inlet or instrument changes at some point during time period, multiple datasets will be returned,
        one for each inlet/instrument.

        Args:    
            site:
                Site of interest e.g. MHD for the Mace Head site.
            species_in (str) :
                Species identifier e.g. ch4 for methane.
            start_date: 
                Output start date in a format that Pandas can interpret
            end_date: 
                Output end date in a format that Pandas can interpret
            inlet: 
                Inlet label. If you want to merge all inlets, use "all"
            average:
                Averaging period for each dataset.
                Each value should be a string of the form e.g. "2H", "30min" (should match pandas offset aliases format).
            keep_missing:
                Whether to keep missing data points or drop them.
            network: 
                Network for the site/instrument (must match number of sites).
            instrument:
                Specific instrument for the site (must match number of sites). 
            calibration_scale:
                Convert to this calibration scale (original scale and new scale must both be in acrg_obs_scale_convert.csv)
        Returns:
            list: List of xarray.Datasets
    """
    from pandas import Timestamp, Timedelta
    import numpy as np
    from xarray import concat as xr_concat
    from openghg.localclient import Search
    from openghg.util import load_json

    site_info = load_json(filename="acrg_site_info.json")
    site = site.upper()

    if site not in site_info:
        raise ValueError(f"No site called {site}, please enter a valid site name.")

    # Ensure we have the Timestamps we expect
    if start_date is not None:
        start_date = Timestamp(start_date)
    if end_date is not None:
        end_date = Timestamp(end_date)

    # Find the correct synonym for the passed species
    species = synonyms(species)

    search = Search()

    results = search.search(
        species=species, locations=site, inlet=inlet, instrument=instrument, start_date=start_date, end_date=end_date,
    )

    # Retrieve all the data found
    selected_keys = list(results.keys())
    retrieved_data = search.retrieve(selected_keys=selected_keys)

    obs_files = []

    for key, dateranges in retrieved_data.items():
        for d in dateranges:
            split_dates = d.split("_")

            start_date = Timestamp(split_dates[0])
            end_date = Timestamp(split_dates[1])

            data = dateranges[d]

            if average is not None:
                if keep_missing is True:

                    # Create a dataset with one element and NaNs to prepend or append
                    ds_single_element = data[{"time": 0}]

                    for v in ds_single_element.variables:
                        if v != "time":
                            ds_single_element[v].values = np.nan

                    ds_concat = []
                    # Pad with an empty entry at the start date
                    if min(data.time) > Timestamp(start_date):
                        ds_single_element_start = ds_single_element.copy()
                        ds_single_element_start.time.values = Timestamp(start_date)
                        ds_concat.append(ds_single_element_start)

                    ds_concat.append(data)

                    # Pad with an empty entry at the end date
                    if max(data.time) < Timestamp(end_date):
                        ds_single_element_end = ds_single_element.copy()
                        ds_single_element_end.time.values = Timestamp(end_date) - Timedelta("1ns")
                        ds_concat.append(ds_single_element_end)

                    data = xr_concat(ds_concat, dim="time")

                    # Now sort to get everything in the right order
                    data = data.sortby("time")

                # First do a mean resample on all variables
                ds_resampled = data.resample(time=average, keep_attrs=True).mean(skipna=False)
                # keep_attrs doesn't seem to work for some reason, so manually copy
                ds_resampled.attrs = data.attrs.copy()

                # For some variables, need a different type of resampling
                for var in data.variables:
                    if "repeatability" in var:
                        ds_resampled[var] = (
                            np.sqrt((data[var] ** 2).resample(time=average).sum()) / data[var].resample(time=average).count()
                        )

                    elif "variability" in var:
                        # Calculate std of 1 min mf obs in av period as new vmf
                        ds_resampled[var] = data[var].resample(time=average, keep_attrs=True).std(skipna=False)

                    # Copy over some attributes
                    if "long_name" in data[var].attrs:
                        ds_resampled[var].attrs["long_name"] = data[var].attrs["long_name"]
                    if "units" in data[var].attrs:
                        ds_resampled[var].attrs["units"] = data[var].attrs["units"]

                data = ds_resampled.copy()

            # Rename variables
            rename = {}

            for var in data.variables:
                if var.lower() == species.lower():
                    rename[var] = "mf"
                if "repeatability" in var:
                    rename[var] = "mf_repeatability"
                if "variability" in var:
                    rename[var] = "mf_variability"
                if "number_of_observations" in var:
                    rename[var] = "mf_number_of_observations"
                if "status_flag" in var:
                    rename[var] = "status_flag"
                if "integration_flag" in var:
                    rename[var] = "integration_flag"

            data = data.rename_vars(rename)

            data.attrs["species"] = species
            if "Calibration_scale" in data.attrs:
                data.attrs["scale"] = data.attrs.pop("Calibration_scale")

            if calibration_scale is not None:
                data = scale_convert(data, species, calibration_scale)

            metadata = data.attrs
            obs_data = ObsData(name=key, data=data, metadata=data.attrs)

            obs_files.append(obs_data)

    # Now check if the units match for each of the observation Datasets
    units = set([f.data.mf.attrs["units"] for f in obs_files])
    if len(units) > 1:
        raise ValueError(
            f"Units do not match for these observation Datasets {[(f.mf.attrs['units'],f.attrs['filename']) for f in obs_files]}"
        )

    scales = set([f.data.attrs["scale"] for f in obs_files])
    if len(scales) > 1:
        print(f"Scales do not match for these observation Datasets {[(f.attrs['scale'],f.attrs['filename']) for f in obs_files]}")
        print("Suggestion: set calibration_scale to convert scales")

    return obs_files


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
    from openghg.util import load_json

    # Load in the species data
    species_data = load_json(filename="acrg_species_info.json")

    # First test whether site matches keys (case insensitive)
    matched_strings = [k for k in species_data if k.upper() == species.upper()]

    # Used to access the alternative names in species_data
    alt_label = "alt"

    # If not found, search synonyms
    if not matched_strings:
        for key in species_data:
            # Iterate over the alternative labels and check for a match
            matched_strings = [s for s in species_data[key][alt_label] if s.upper() == species.upper()]

            if matched_strings:
                matched_strings = [key]
                break

    if matched_strings:
        updated_species = matched_strings[0]

        return updated_species
    else:
        raise ValueError(f"Unable to find synonym for species {species}")


def scale_convert(data: Dataset, species: str, to_scale: str) -> Dataset:
    """ Convert to a new calibration scale

        Args:
            data: Must contain an mf variable (mole fraction), and scale must be in global attributes
            species: species name
            to_scale: Calibration scale to convert to
        Returns:
            xarray.Dataset: Dataset with mole fraction data scaled
    """
    from pandas import read_csv
    from numexpr import evaluate
    from openghg.util import get_datapath

    # If scale is already correct, return
    ds_scale = data.attrs["scale"]
    if ds_scale == to_scale:
        return data

    scale_convert_filepath = get_datapath("acrg_obs_scale_convert.csv")

    scale_converter = read_csv(scale_convert_filepath)
    scale_converter_scales = scale_converter[scale_converter.isin([species.upper(), ds_scale, to_scale])][
        ["species", "scale1", "scale2"]
    ].dropna(axis=0, how="any")

    if len(scale_converter_scales) == 0:
        raise ValueError(
            f"Scales {ds_scale} and {to_scale} are not both in any one row in acrg_obs_scale_convert.csv for species {species}"
        )
    elif len(scale_converter_scales) > 1:
        raise ValueError(f"Duplicate rows in acrg_obs_scale_convert.csv?")
    else:
        row = scale_converter_scales.index[0]

    converter = scale_converter.loc[row]

    if to_scale == converter["scale1"]:
        direction = "2to1"
    else:
        direction = "1to2"

    # flake8: noqa: F841
    # scale_convert file has variable X in equations, so let's create it
    X = 1.0
    scale_factor = evaluate(converter[direction])
    data["mf"].values *= scale_factor

    data.attrs["scale"] = to_scale

    return data
