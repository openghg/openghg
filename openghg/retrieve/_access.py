import logging
from typing import Any, Union

from openghg.dataobjects._basedata import _BaseData  # TODO: expose this type?
from openghg.dataobjects import (
    BoundaryConditionsData,
    FluxData,
    FootprintData,
    ObsColumnData,
    ObsData,
)
from openghg.types import SearchError
from openghg.util import combine_and_elevate_inlet
from pandas import Timestamp
from xarray import Dataset

logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

DataTypes = Union[BoundaryConditionsData, FluxData, FootprintData, ObsColumnData, ObsData]
multDataTypes = Union[
    list[BoundaryConditionsData], list[FluxData], list[FootprintData], list[ObsColumnData], list[ObsData]
]


def _get_generic(
    combine_multiple_inlets: bool = False,
    ambig_check_params: list | None = None,
    **kwargs: Any,
) -> _BaseData:
    """Perform a search and create a dataclass object with the results if any are found.

    Args:
        data_class: Type of dataobject to create
        elevate_inlets: Elevate the inlet attribute to be a variable within the Dataset
        ambig_check_params: Parameters to check and print if result is ambiguous.
        kwargs: Additional search terms
    Returns:
        dataclass
    """
    from openghg.retrieve import search

    results = search(**kwargs)

    keyword_string = _create_keyword_string(**kwargs)
    if not results:
        err_msg = f"Unable to find results for {keyword_string}"
        logger.exception(err_msg)
        raise SearchError(err_msg)

    # TODO: UPDATE THIS - just use retrieve when retrieve_all is removed.
    retrieved_data = results.retrieve_all()

    if retrieved_data is None:
        err_msg = f"Unable to retrieve results for {keyword_string}"
        logger.exception(err_msg)
        raise SearchError(err_msg)
    elif isinstance(retrieved_data, list) and len(retrieved_data) > 1:
        if combine_multiple_inlets:
            result = combine_and_elevate_inlet(retrieved_data)
        else:
            param_diff_formatted = _metadata_difference_formatted(
                data=retrieved_data, params=ambig_check_params
            )
            err_msg = f"""
            Multiple entries found for input parameters for {keyword_string}.
            Parameter differences:
            {param_diff_formatted}
            Please supply additional parameters or set ranking.
            """
            logger.exception(err_msg)
            raise SearchError(err_msg)
    elif isinstance(retrieved_data, list):
        result = retrieved_data[0]
    else:
        result = retrieved_data

    return result


def get_obs_surface(
    site: str,
    species: str,
    inlet: str | slice | None = None,
    height: str | None = None,
    start_date: str | Timestamp | None = None,
    end_date: str | Timestamp | None = None,
    average: str | None = None,
    network: str | None = None,
    instrument: str | None = None,
    calibration_scale: str | None = None,
    rename_vars: bool = True,
    keep_missing: bool = False,
    skip_ranking: bool = False,
    **kwargs: Any,
) -> ObsData | None:
    """This is the equivalent of the get_obs function from the ACRG repository.

    Usage and return values are the same whilst implementation may differ.

    Args:
        site: Site of interest e.g. MHD for the Mace Head site.
        species: Species identifier e.g. ch4 for methane.
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        inlet: Inlet height above ground level in metres; This can be a single value or `slice(lower, upper)`
            can be used to search for a range of values. `lower` and `upper` can be int, float, or strings
            such as '100m'.
        height: Alias for inlet
        average: Averaging period for each dataset. Each value should be a string of
        the form e.g. "2H", "30min" (should match pandas offset aliases format).
        keep_missing: Keep missing data points or drop them.
        network: Network for the site/instrument (must match number of sites).
        instrument: Specific instrument for the sipte (must match number of sites).
        calibration_scale: Convert to this calibration scale
        rename_vars: Rename variables from species names to use "mf" explictly.
        kwargs: Additional search terms
    Returns:
        ObsData or None: ObsData object if data found, else None
    """
    import numpy as np
    from openghg.util import (
        format_inlet,
        get_site_info,
        synonyms,
        timestamp_tzaware,
    )
    from pandas import Timedelta

    # Allow height to be an alias for inlet but we do not expect height
    # to be within the metadata (for now)
    if inlet is None and height is not None:
        inlet = height
    inlet = format_inlet(inlet)
    if species is not None:
        species = synonyms(species)

    data_type = "surface"

    # Allow height to be an alias for inlet but we do not expect height
    # to be within the metadata (for now)
    if inlet is None and height is not None:
        inlet = height
    inlet = format_inlet(inlet)

    site_data = get_site_info()
    site = site.upper()

    # TODO: Evaluate this constraint - how do we want to handle and incorporate new sites?
    if site not in site_data:
        raise ValueError(f"No site called {site}, please enter a valid site name.")

    surface_keywords = {
        "site": site,
        "species": species,
        "inlet": inlet,
        "start_date": start_date,
        "end_date": end_date,
        "network": network,
        "instrument": instrument,
        "data_type": data_type,
    }
    surface_keywords.update(kwargs)

    # # Get the observation data
    # obs_results = search_surface(**surface_keywords)
    retrieved_data = _get_generic(
        combine_multiple_inlets=isinstance(inlet, slice),  # if range passed for inlet, try to combine
        ambig_check_params=["inlet", "network", "instrument"],
        **surface_keywords,  # type: ignore
    )

    data = retrieved_data.data

    if data.attrs["inlet"] == "multiple":
        data.attrs["inlet_height_magl"] = "multiple"
        retrieved_data.metadata["inlet"] = "multiple"

    if start_date is not None and end_date is not None:
        # Check if underlying data is timezone aware.
        data_time_index = data.indexes["time"]
        tzinfo = data_time_index.tzinfo

        if tzinfo:
            start_date_filter = timestamp_tzaware(start_date)
            end_date_filter = timestamp_tzaware(end_date)
        else:
            start_date_filter = Timestamp(start_date)
            end_date_filter = Timestamp(end_date)

        end_date_filter_exclusive = end_date_filter - Timedelta(
            1, unit="nanosecond"
        )  # Deduct 1 ns to make the end day (date) exclusive.

        # Slice the data to only cover the dates we're interested in
        data = data.sel(time=slice(start_date_filter, end_date_filter_exclusive))

    try:
        start_date_data = timestamp_tzaware(data.time[0].values)
        end_date_data = timestamp_tzaware(data.time[-1].values)
    except AttributeError:
        raise AttributeError("This dataset does not have a time attribute, unable to read date range")
    except IndexError:
        return None

    if average is not None:
        # We need to compute the value here for the operations done further down
        logger.info("Loading Dataset data into memory for resampling operations.")
        data = data.compute()
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
        data_variables: list[str] = [str(v) for v in data.variables]

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
        if "long_name" in data[species].attrs:
            ds_resampled[f"{species}_variability"].attrs[
                "long_name"
            ] = f"{data[species].attrs['long_name']}_variability"

        if "units" in data[species].attrs:
            ds_resampled[f"{species}_variability"].attrs["units"] = data[species].attrs["units"]

        # Resampling may introduce NaNs, so remove, if not keep_missing
        if keep_missing is False:
            ds_resampled = ds_resampled.dropna(dim="time")

        data = ds_resampled

    # Rename variables
    if rename_vars:
        rename: dict[str, str] = {}

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


def get_obs_column(
    species: str,
    max_level: int,
    satellite: str | None = None,
    domain: str | None = None,
    selection: str | None = None,
    site: str | None = None,
    network: str | None = None,
    instrument: str | None = None,
    platform: str = "satellite",
    start_date: str | Timestamp | None = None,
    end_date: str | Timestamp | None = None,
    return_mf: bool = True,
    **kwargs: Any,
) -> ObsColumnData:
    """Extract available column data from the object store using keywords.

    Args:
        species: Species name
        source: Source name
        domain: Domain e.g. EUROPE
        start_date: Start date
        end_date: End date
        time_resolution: One of ["standard", "high"]
        return_mf: Return mole fraction rather than column data. Default=True
        kwargs: Additional search terms
    Returns:
        ObsColumnData: ObsColumnData object
    """
    obs_data = _get_generic(
        species=species,
        satellite=satellite,
        domain=domain,
        selection=selection,
        site=site,
        network=network,
        instrument=instrument,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        data_type="column",
        **kwargs,
    )

    if return_mf:

        if max_level > max(obs_data.data.lev.values) + 1:
            logger.warning(
                f"passed max level is above max level in data ({max(obs_data.data.lev.values)+1}). Defaulting to highest level"
            )
            max_level = max(obs_data.data.lev.values) + 1

        ## processing taken from acrg/acrg/obs/read.py get_gosat()
        lower_levels = list(range(0, max_level))

        prior_factor = (
            obs_data.data.pressure_weights[dict(lev=list(lower_levels))]
            * (1.0 - obs_data.data.xch4_averaging_kernel[dict(lev=list(lower_levels))])
            * obs_data.data.ch4_profile_apriori[dict(lev=list(lower_levels))]
        ).sum(dim="lev")

        upper_levels = list(range(max_level, len(obs_data.data.lev.values)))
        prior_upper_level_factor = (
            obs_data.data.pressure_weights[dict(lev=list(upper_levels))]
            * obs_data.data.ch4_profile_apriori[dict(lev=list(upper_levels))]
        ).sum(dim="lev")

        obs_data.data["mf_prior_factor"] = prior_factor
        obs_data.data["mf_prior_upper_level_factor"] = prior_upper_level_factor
        obs_data.data["mf"] = (
            obs_data.data.xch4 - obs_data.data.mf_prior_factor - obs_data.data.mf_prior_upper_level_factor
        )
        obs_data.data["mf_repeatability"] = obs_data.data.xch4_uncertainty

        # rt17603: 06/04/2018 Added drop variables to ensure lev and id dimensions are also dropped, Causing problems in footprints_data_merge() function
        drop_data_vars = [
            "xch4",
            "xch4_uncertainty",
            "lon",
            "lat",
            "ch4_profile_apriori",
            "xch4_averaging_kernel",
            "pressure_levels",
            "pressure_weights",
            "exposure_id",
        ]
        drop_coords = ["lev", "id"]

        for dv in drop_data_vars:
            if dv in obs_data.data.data_vars:
                obs_data.data = obs_data.data.drop_vars(dv)
        for coord in drop_coords:
            if coord in obs_data.data.coords:
                obs_data.data = obs_data.data.drop_vars(coord)

        obs_data.data.attrs["max_level"] = max_level
        if species.upper() == "CH4":
            # obs_data.data.mf.attrs["units"] = "1e-9"
            obs_data.data.attrs["species"] = "CH4"
        if species.upper() == "CO2":
            # obs_data.data.mf.attrs["units"] = "1e-6"
            obs_data.data.attrs["species"] = "CO2"

        # obs_data.data.attrs["scale"] = "GOSAT"

        obs_data.metadata["transforms"] = (
            f"For creating mole fraction, used apriori data for levels above max_level={max_level}"
        )

    obs_data.data = obs_data.data.sortby("time")

    return ObsColumnData(data=obs_data.data, metadata=obs_data.metadata)


def get_flux(
    species: str,
    source: str,
    domain: str,
    database: str | None = None,
    database_version: str | None = None,
    model: str | None = None,
    start_date: str | Timestamp | None = None,
    end_date: str | Timestamp | None = None,
    time_resolution: str | None = None,
    **kwargs: Any,
) -> FluxData:
    """The flux function reads in all flux files for the domain and species as an xarray Dataset.
    Note that at present ALL flux data is read in per species per domain or by emissions name.
    To be consistent with the footprints, fluxes should be in mol/m2/s.

    Args:
        species: Species name
        source: Source name
        domain: Domain e.g. EUROPE
        start_date: Start date
        end_date: End date
        time_resolution: One of ["standard", "high"]
        kwargs: Additional search terms
    Returns:
        FluxData: FluxData object
    """
    em_data = _get_generic(
        species=species,
        source=source,
        domain=domain,
        database=database,
        database_version=database_version,
        model=model,
        time_resolution=time_resolution,
        start_date=start_date,
        end_date=end_date,
        data_type="flux",
        **kwargs,
    )

    em_ds = em_data.data
    # Check for level coordinate. If one level, assume surface and drop
    if "lev" in em_ds.coords:
        if len(em_ds.lev) > 1:
            raise ValueError("Error: More than one flux level")

        em_ds = em_ds.drop_vars(names="lev")

    return FluxData(data=em_data.data, metadata=em_data.metadata)


def get_bc(
    species: str,
    domain: str,
    bc_input: str | None = None,
    start_date: str | Timestamp | None = None,
    end_date: str | Timestamp | None = None,
    **kwargs: Any,
) -> BoundaryConditionsData:
    """Get boundary conditions for a given species, domain and bc_input name.

    Args:
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions e.g. EUROPE
        start_date: Start date
        end_date: End date
    Returns:
        BoundaryConditionsData: BoundaryConditionsData object
    """
    bc_data = _get_generic(
        species=species,
        bc_input=bc_input,
        domain=domain,
        start_date=start_date,
        end_date=end_date,
        data_type="boundary_conditions",
        **kwargs,
    )

    return BoundaryConditionsData(data=bc_data.data, metadata=bc_data.metadata)


def get_footprint(
    site: str,
    domain: str,
    inlet: str | None = None,
    height: str | None = None,
    model: str | None = None,
    start_date: str | Timestamp | None = None,
    end_date: str | Timestamp | None = None,
    species: str | None = None,
    **kwargs: Any,
) -> FootprintData:
    """Get footprints from one site.

    Args:
        site: The name of the site given in the footprints. This often matches
              to the site name but  if the same site footprints are run with a
              different met and they are named slightly differently from the obs
              file. E.g. site="DJI", site_modifier = "DJI-SAM" -
              station called DJI, footprints site called DJI-SAM
        domain : Domain name for the footprints
        inlet: Height above ground level in metres
        height: Alias for inlet
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        species: Species identifier e.g. "co2" for carbon dioxide. Only needed
                 if species needs a modified footprints from the typical 30-day
                 footprints appropriate for a long-lived species (like methane)
                 e.g. for high time resolution (co2) or is a short-lived species.
        kwargs: Additional search terms
    Returns:
        FootprintData: FootprintData dataclass
    """
    from openghg.util import clean_string, format_inlet, synonyms

    # Find the correct synonym for the passed species
    if species is not None:
        species = clean_string(synonyms(species))

    # Allow inlet or height to be specified, both or either may be included
    # within the metadata so could use either to search
    inlet = format_inlet(inlet)
    height = format_inlet(height)

    fp_data = _get_generic(
        site=site,
        domain=domain,
        inlet=inlet,
        height=height,
        model=model,
        start_date=start_date,
        end_date=end_date,
        species=species,
        data_type="footprints",
        **kwargs,
    )

    return FootprintData(data=fp_data.data, metadata=fp_data.metadata)

    # TODO: Could incorporate this somewhere? Setting species to INERT?
    # if species is None:
    #     species = metadata.get("species", "INERT")


def _scale_convert(data: Dataset, species: str, to_scale: str) -> Dataset:
    """Convert to a new calibration scale

    Args:
        data: Must contain an mf variable (mole fraction), and scale must be in global attributes
        species: species name
        to_scale: Calibration scale to convert to
    Returns:
        xarray.Dataset: Dataset with mole fraction data scaled
    """
    from numexpr import evaluate
    from openghg.util import get_datapath
    from pandas import read_csv

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
        raise ValueError("Duplicate rows in acrg_obs_scale_convert.csv?")
    else:
        row = scale_converter_scales.index[0]

    converter = scale_converter.loc[row]

    direction = "2to1" if to_scale == converter["scale1"] else "1to2"

    # flake8: noqa: F841
    # scale_convert file has variable X in equations, so let's create it
    X = 1.0
    scale_factor = evaluate(converter[direction])
    data["mf"].values *= scale_factor

    data.attrs["scale"] = to_scale

    return data


def _create_keyword_string(**kwargs: Any) -> str:
    """Create a formatted string for supplied keyword values. This will ignore
    keywords where the value is None.
    This is used for printing details of keywords passed to the search functions.
    """
    used_keywords = {key: value for key, value in kwargs.items() if value is not None}
    keyword_string = ", ".join([f"{key}='{value}'" for key, value in used_keywords.items()])

    return keyword_string


def _metadata_difference(
    data: multDataTypes, params: list | None = None, print_output: bool = True
) -> dict[str, list]:
    """Check differences between metadata for returned data objects. Note this will
    only look at differences between values which are strings (not lists, floats etc.)

    Args:
        data: Multiple data objects e.g. multiple ObsData as a list
        params: Specific metadata parameters to check. If None all parameters will be checked
        print_output: Summarise and print output to screen.

    Returns:
        Dict[str, list]: Keys and lists of values from the metadata with differences.
    """
    # Extract metadata dictionaries from each data object in list
    metadata = [d.metadata for d in data]

    if not metadata:
        err_msg = "Unable to read metadata."
        logger.exception(err_msg)
        raise ValueError(err_msg)

    # Creating multiple metadata dictionaries to be compared
    # - Check if only selected parameters be included
    if params is not None:
        metadata = [{param: m[param] for param in params} for m in metadata]

    # - Check if some parameters should be explicitly ignored and not compared
    ignore_params = ["uuid", "data_owner", "data_owner_email"]
    if ignore_params is not None:
        metadata = [{key: value for key, value in m.items() if key not in ignore_params} for m in metadata]

    # - Extract string values  only from the underlying metadata dictionaries
    metadata = [{key: value for key, value in m.items() if isinstance(value, str)} for m in metadata]

    # Select first metadata dictionary from list and use this to compare to others
    # - Look at difference between first metadata dict and other metadata dicts
    metadata0 = metadata[0]
    difference = []
    for metadata_compare in metadata[1:]:
        try:
            metadata_diff = set(metadata0.items()) - set(metadata_compare.items())
        except TypeError:
            logger.warning("Unable to compare metadata between ambiguous results")
            return {}
        else:
            difference.extend(list(metadata_diff))
    # - Select parameter names for values which are different between metadata dictionaries
    param_difference = list(set([d[0] for d in difference]))

    # ignore_params = ["data_owner", "data_owner_email"]
    # for iparam in ignore_params:
    #     try:
    #         param_difference.remove(iparam)
    #     except ValueError:
    #         continue

    # - Collate summary of differences as a dictionary which maps as param: list of values
    summary_difference: dict[str, list] = {}
    for param in param_difference:
        summary_difference[param] = []
        if print_output:
            logger.info(f" {param}: ")
        for m in metadata:
            value = m.get(param, "NOT PRESENT")
            summary_difference[param].append(value)
            if print_output:
                logger.info(f" '{value}', ")
        if print_output:
            logger.info("\n")  # print new line

    # if print_output:
    #     print("Datasets contain:")
    #     for param in param_difference:
    #         print(f" {param}: ", end="")
    #         for m in metadata:
    #             print(f" '{m[param]}', ", end="")
    #         print()  # print new line

    return summary_difference


def _metadata_difference_formatted(
    data: multDataTypes, params: list | None = None, print_output: bool = True
) -> str:
    """Create formatted string for the difference in metadata between input objects.

    Args:
        data : Multiple data objects e.g. multiple ObsData as a list
        params : Specific metadata parameters to check. If None all parameters will be checked
        print_output : Summarise and print output to screen.

    Returns:
        str : Formatted string summarising differences in keys and sets of values
              from the metadata.
    """
    param_difference = _metadata_difference(data, params, print_output)
    formatted = "\n".join([f" - {key}: {', '.join(values)}" for key, values in param_difference.items()])
    return formatted
