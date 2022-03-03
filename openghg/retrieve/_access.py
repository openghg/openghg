from xarray import Dataset
from typing import Dict, List, Optional, Union
from pandas import Timestamp
from openghg.dataobjects import ObsData, FootprintData, FluxData

__all__ = ["get_obs_surface", "get_footprint", "get_flux"]


def get_obs_surface(
    site: str,
    species: str,
    inlet: str = None,
    start_date: Optional[Union[str, Timestamp]] = None,
    end_date: Optional[Union[str, Timestamp]] = None,
    average: Optional[str] = None,
    network: Optional[str] = None,
    instrument: Optional[str] = None,
    calibration_scale: Optional[str] = None,
    keep_missing: Optional[bool] = False,
    skip_ranking: Optional[bool] = False,
) -> ObsData:
    """Get measurements from one site.

    Args:
        site: Site of interest e.g. MHD for the Mace Head site.
        species: Species identifier e.g. ch4 for methane.
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        inlet: Inlet label
        average: Averaging period for each dataset. Each value should be a string of
        the form e.g. "2H", "30min" (should match pandas offset aliases format).
        keep_missing: Keep missing data points or drop them.
        network: Network for the site/instrument (must match number of sites).
        instrument: Specific instrument for the site (must match number of sites).
        calibration_scale: Convert to this calibration scale
    Returns:
        ObsData: ObsData object
    """
    from pandas import Timestamp, Timedelta
    import numpy as np
    from xarray import concat as xr_concat
    from openghg.retrieve import search
    from openghg.store import recombine_datasets
    from openghg.util import clean_string, load_json, timestamp_tzaware

    site_info = load_json(filename="acrg_site_info.json")
    site = site.upper()

    if site not in site_info:
        raise ValueError(f"No site called {site}, please enter a valid site name.")

    # Find the correct synonym for the passed species
    species = clean_string(_synonyms(species))

    # Get the observation data
    obs_results = search(
        site=site,
        species=species,
        inlet=inlet,
        start_date=start_date,
        end_date=end_date,
        instrument=instrument,
        find_all=True,
        skip_ranking=skip_ranking,
    )

    if not obs_results:
        raise ValueError(f"Unable to find results for {species} at {site}")

    retrieved_data: ObsData = obs_results.retrieve(site=site, species=species, inlet=inlet)  # type: ignore
    data = retrieved_data.data

    if data.attrs["inlet"] == "multiple":
        data.attrs["inlet_height_magl"] = "multiple"
        retrieved_data.metadata["inlet"] = "multiple"

    if start_date is not None and end_date is not None:
        start_date_tzaware = timestamp_tzaware(start_date)
        end_date_tzaware = timestamp_tzaware(end_date)
        end_date_tzaware_exclusive = end_date_tzaware - Timedelta(
            1, unit="nanosecond"
        )  # Deduct 1 ns to make the end day (date) exclusive.

        # Slice the data to only cover the dates we're interested in
        data = data.sel(time=slice(start_date_tzaware, end_date_tzaware_exclusive))

    try:
        start_date_data = timestamp_tzaware(data.time[0].values)
        end_date_data = timestamp_tzaware(data.time[-1].values)
    except AttributeError:
        raise AttributeError("This dataset does not have a time attribute, unable to read date range")
    except IndexError:
        return ObsData(data=Dataset(), metadata={})

    if average is not None:
        # GJ - 2021-03-09
        # TODO - check by RT

        # # Average the Dataset over a given period
        # if keep_missing is True:
        #     # Create a dataset with one element and NaNs to prepend or append
        #     ds_single_element = data[{"time": 0}]

        #     for v in ds_single_element.variables:
        #         if v != "time":
        #             ds_single_element[v].values = np.nan

        #     ds_concat = []

        #     # Pad with an empty entry at the start date
        #     if timestamp_tzaware(data.time.min()) > start_date:
        #         ds_single_element_start = ds_single_element.copy()
        #         ds_single_element_start.time.values = Timestamp(start_date)
        #         ds_concat.append(ds_single_element_start)

        #     ds_concat.append(data)

        #     # Pad with an empty entry at the end date
        #     if data.time.max() < Timestamp(end_date):
        #         ds_single_element_end = ds_single_element.copy()
        #         ds_single_element_end.time.values = Timestamp(end_date) - Timedelta("1ns")
        #         ds_concat.append(ds_single_element_end)

        #     data = xr_concat(ds_concat, dim="time")

        #     # Now sort to get everything in the right order
        #     data = data.sortby("time")

        # First do a mean resample on all variables
        ds_resampled = data.resample(time=average).mean(skipna=False, keep_attrs=True)
        # keep_attrs doesn't seem to work for some reason, so manually copy
        ds_resampled.attrs = data.attrs.copy()

        average_in_seconds = Timedelta(average).total_seconds()
        ds_resampled.attrs["averaged_period"] = average_in_seconds
        ds_resampled.attrs["averaged_period_str"] = average

        # For some variables, need a different type of resampling
        data_variables: List[str] = [str(v) for v in data.variables]

        for var in data_variables:
            if "repeatability" in var:
                ds_resampled[var] = (
                    np.sqrt((data[var] ** 2).resample(time=average).sum())
                    / data[var].resample(time=average).count()
                )

            # Copy over some attributes
            if "long_name" in data[var].attrs:
                ds_resampled[var].attrs["long_name"] = data[var].attrs["long_name"]

            if "units" in data[var].attrs:
                ds_resampled[var].attrs["units"] = data[var].attrs["units"]

        # Create a new variability variable, containing the standard deviation within the resampling period
        ds_resampled[f"{species}_variability"] = (
            data[species].resample(time=average).std(skipna=False, keep_attrs=True)
        )

        # If there are any periods where only one measurement was resampled, just use the median variability
        ds_resampled[f"{species}_variability"][ds_resampled[f"{species}_variability"] == 0.0] = ds_resampled[
            f"{species}_variability"
        ].median()

        # Create attributes for variability variable
        ds_resampled[f"{species}_variability"].attrs["long_name"] = f"{data.attrs['long_name']}_variability"

        ds_resampled[f"{species}_variability"].attrs["units"] = data[species].attrs["units"]

        # Resampling may introduce NaNs, so remove, if not keep_missing
        if keep_missing is False:
            ds_resampled = ds_resampled.dropna(dim="time")

        data = ds_resampled

    # Rename variables
    rename: Dict[str, str] = {}

    data_variables = [str(v) for v in data.variables]
    for var in data_variables:
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

    data = data.rename_vars(rename)  # type: ignore

    data.attrs["species"] = species

    if "calibration_scale" in data.attrs:
        data.attrs["scale"] = data.attrs.pop("calibration_scale")

    if calibration_scale is not None:
        data = _scale_convert(data, species, calibration_scale)

    metadata = retrieved_data.metadata
    metadata.update(data.attrs)

    obs_data = ObsData(data=data, metadata=metadata)

    # It doesn't make sense to do this now as we've only got a single Dataset
    # # Now check if the units match for each of the observation Datasets
    # units = set((f.data.mf.attrs["units"] for f in obs_files))
    # scales = set((f.data.attrs["scale"] for f in obs_files))

    # if len(units) > 1:
    #     raise ValueError(
    #         f"Units do not match for these observation Datasets {[(f.mf.attrs['station_long_name'],f.attrs['units']) for f in obs_files]}"
    #     )

    # if len(scales) > 1:
    #     print(
    #         f"Scales do not match for these observation Datasets {[(f.mf.attrs['station_long_name'],f.attrs['units']) for f in obs_files]}"
    #     )
    #     print("Suggestion: set calibration_scale to convert scales")

    return obs_data


def get_flux(
    species: str,
    source: str,
    domain: str,
    start_date: Optional[Timestamp] = None,
    end_date: Optional[Timestamp] = None,
    time_resolution: Optional[str] = None,
) -> FluxData:
    """
    The flux function reads in all flux files for the domain and species as an xarray Dataset.
    Note that at present ALL flux data is read in per species per domain or by emissions name.
    To be consistent with the footprints, fluxes should be in mol/m2/s.

    Args:
        species: Species name
        source: Source name
        domain: Domain e.g. EUROPE
        start_date: Start date
        end_date: End date
        time_resolution: One of ["standard", "high"]
    Returns:
        FluxData: FluxData object
    """
    from openghg.retrieve import search
    from openghg.store import recombine_datasets
    from openghg.util import timestamp_epoch, timestamp_now

    if start_date is None:
        start_date = timestamp_epoch()
    if end_date is None:
        end_date = timestamp_now()

    results: Dict = search(
        species=species,
        source=source,
        domain=domain,
        time_resolution=time_resolution,
        start_date=start_date,
        end_date=end_date,
        data_type="emissions",
    )  # type: ignore

    if not results:
        raise ValueError(f"Unable to find flux data for {species} from {source}")

    try:
        em_key = list(results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any footprints data for {domain} for {species}.")

    data_keys = results[em_key]["keys"]
    metadata = results[em_key]["metadata"]

    em_ds = recombine_datasets(keys=data_keys, sort=False)

    # Check for level coordinate. If one level, assume surface and drop
    if "lev" in em_ds.coords:
        if len(em_ds.lev) > 1:
            raise ValueError("Error: More than one flux level")

        em_ds = em_ds.drop_vars(names="lev")

    if species is None:
        species = metadata.get("species", "NA")

    return FluxData(
        data=em_ds,
        metadata=metadata,
        flux={},
        bc={},
        species=species,
        scales="FIXME",
        units="FIXME",
    )


def get_footprint(
    site: str,
    domain: str,
    height: str,
    model: str = None,
    start_date: Timestamp = None,
    end_date: Timestamp = None,
    species: str = None,
) -> FootprintData:
    """
    Get footprints from one site.

    Args:
        site: The name of the site given in the footprints. This often matches
              to the site name but  if the same site footprints are run with a
              different met and they are named slightly differently from the obs
              file. E.g. site="DJI", site_modifier = "DJI-SAM" -
              station called DJI, footprints site called DJI-SAM
        domain : Domain name for the footprints
        height: Height of inlet in metres
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        species: Species identifier e.g. "co2" for carbon dioxide. Only needed
                 if species needs a modified footprints from the typical 30-day
                 footprints appropriate for a long-lived species (like methane)
                 e.g. for high time resolution (co2) or is a short-lived species.
    Returns:
        FootprintData: FootprintData dataclass
    """
    from openghg.store import recombine_datasets
    from openghg.retrieve import search
    from openghg.dataobjects import FootprintData

    results = search(
        site=site,
        domain=domain,
        height=height,
        start_date=start_date,
        end_date=end_date,
        species=species,
        data_type="footprints",
    )  # type: ignore
    # Get the footprints data
    # if species is not None:
    # else:
    #     results = search(
    #         site=site,
    #         domain=domain,
    #         domain=domain,
    #         height=height,
    #         start_date=start_date,
    #         end_date=end_date,
    #         data_type="footprints",
    #     )  # type: ignore

    try:
        fp_site_key = list(results.keys())[0]
    except IndexError:
        if species is not None:
            raise ValueError(
                f"Unable to find any footprints data for {site} at a height of {height} for species {species}."
            )
        else:
            raise ValueError(f"Unable to find any footprints data for {site} at a height of {height}.")

    keys = results[fp_site_key]["keys"]
    metadata = results[fp_site_key]["metadata"]
    # fp_ds = recombine_datasets(keys=keys, sort=False) # Why did this have sort=False before?
    fp_ds = recombine_datasets(keys=keys, sort=True)

    if species is None:
        species = metadata.get("species", "NA")

    return FootprintData(
        data=fp_ds,
        metadata=metadata,
        flux={},
        bc={},
        species=species,
        scales="FIXME",
        units="FIXME",
    )


def _synonyms(species: str) -> str:
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
        updated_species = str(matched_strings[0])
        return updated_species
    else:
        raise ValueError(f"Unable to find synonym for species {species}")


def _scale_convert(data: Dataset, species: str, to_scale: str) -> Dataset:
    """Convert to a new calibration scale

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
