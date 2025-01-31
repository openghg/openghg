import re
import numpy as np
import pandas as pd
import xarray as xr
import shutil
import pathlib
from pathlib import Path

import importlib.resources
from typing import Union, Optional, Sequence, cast
import logging

from openghg.retrieve import get_obs_surface, get_obs_column
from openghg.types import pathType, optionalPathType

logger = logging.getLogger("openghg.obspack")


def define_obs_types() -> list:
    """
    Define names of obs type folders to be included in an ObsPack.
    """
    obs_types = ["surface-insitu", "surface-flask", "column"]
    return obs_types


def define_get_functions() -> tuple[dict, dict]:
    """
    Define relationship between data type names (for folders) and get functions.

    Returns:
        dict, dict: Folder name to get function mapping and additional arguments
                    which need to be passed to that function.
    """
    get_functions = {
        "surface-insitu": get_obs_surface,
        "surface-flask": get_obs_surface,
        "column": get_obs_column,
    }

    get_fn_arguments = {
        get_obs_surface: {"rename_vars": False},
        get_obs_column: {"return_mf": False},
    }

    return get_functions, get_fn_arguments


def define_filename(
    name_components: list[Union[str, list]],
    metadata: dict,
    obs_type: Optional[str] = None,
    output_path: optionalPathType = None,
    include_version: bool = True,
    data_version: Optional[str] = None,
) -> Path:
    """
    Define filename based on the determined structure. The input name_components determines
    the initial naming strings, extracting these from the metadata.
    Sub-names can be specified within name_components by including these as a list.

    The name_components inputs will be interpreted as follows:
     - list elements (names) will be separated by underscores "_"
     - any sub-list elements (sub-names) will be seperated by dashes "-"

    Args:
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename.
        metadata: Dictionary containing the metadata keys to use
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        output_path: Full output folder path for file
    Returns:
        Path: Full path for filename

    Examples:
        >>> define_filename(["species", "site", "inlet"], metadata: {"species":"ch4", "site":"tac", "inlet":"42m"})
        Path("ch4_tac_42m_v1.nc")

        >>> define_filename([["species", "sector"], "site", "inlet"], metadata: {"species":"ch4", "sector": "anthro", "site":"tac", "inlet":"42m"})
        Path("ch4-anthro_tac_42m_v1.nc")

        >>> define_filename(["species", "site", "inlet"], metadata: {"species":"ch4", "site":"tac", "inlet":"42m", }, obs_type="surface-insitu")
        Path("ch4_tac_42m_surface-insitu_v1.nc")

        >>> define_filename(["species", "site", "inlet"], metadata: {"species":"ch4", "site":"tac", "inlet":"42m", }, obs_type="surface-insitu", output_path="/path/to/output")
        Path("/path/to/output/ch4_tac_42m_surface-insitu_v1.nc")
    """

    name_separator = "_"
    sub_name_separator = "-"
    file_extension = ".nc"

    if output_path is not None:
        output_path = Path(output_path)

    name_details = []
    missing_keys = []
    for name in name_components:

        if isinstance(name, list):
            sub_names = name
        else:
            sub_names = [name]

        sub_name_details = []
        for sub_name in sub_names:
            if sub_name in metadata:
                sub_name_details.append(metadata[sub_name])
            else:
                missing_keys.append(sub_name)
                sub_name_details.append("")

        full_name = sub_name_separator.join(sub_name_details)
        name_details.append(full_name)

    if missing_keys:
        msg = f"Necessary keys missing from stored metadata: {', '.join(missing_keys)}"
        logger.exception(msg)
        raise ValueError(msg)

    if include_version:
        if data_version is None:
            data_version = find_data_version(metadata)
    else:
        data_version = None

    joined_names = name_separator.join(name_details)
    all_name_components = [joined_names, obs_type, data_version]
    final_name_components = [name for name in all_name_components if name is not None]

    filename = Path(f"{name_separator.join(final_name_components)}{file_extension}")

    if output_path is not None:
        filename = output_path / filename

    return filename


def define_surface_filename(
    metadata: dict,
    obs_type: Optional[str] = None,
    output_path: optionalPathType = None,
    include_version: bool = True,
    data_version: Optional[str] = None,
    name_components: Optional[list] = None,
) -> Path:
    """
    Create file name for surface type (surface-flask or surface-insitu)
    data with expected naming convention.

    Args:
        metadata: Dictionary containing metadata values. Expect at least "site",
            "species" and "inlet" to be present.
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        output_path: Full output folder path for file
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. Using the default sets this to ["species", "site", "inlet"]
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into file naming?
    """

    if name_components is None:
        name_components: list[Union[str, list]] = ["species", "site", "inlet"]

    filename = define_filename(
        name_components,
        metadata=metadata,
        obs_type=obs_type,
        include_version=include_version,
        data_version=data_version,
        output_path=output_path,
    )
    return filename


def define_column_filename(
    metadata: dict,
    obs_type: str = "column",
    output_path: optionalPathType = None,
    include_version: bool = True,
    data_version: Optional[str] = None,
    name_components: Optional[list] = None,
) -> Path:
    """
    Create file name for column type data with expected naming convention.

    Args:
        metadata: Dictionary containing metadata values. Expect at least "platform"
            and "species" to be present.
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        output_path: Full output folder path for file
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. Using the default uses the platform to determine the naming.
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into naming?
    """

    if name_components is None:
        try:
            platform = metadata["platform"]
        except KeyError:
            msg = "Expect 'platform' key to be included for 'column' data. Please check metadata"
            logger.exception(msg)
            raise ValueError(msg)

        if platform == "site":
            name_components: list[Union[str, list]] = ["species", "site", "platform"]
        elif platform == "satellite":
            name_components = ["species"]

            if "satellite" in metadata:
                sub_name_components = ["satellite"]
                if "selection" in metadata:
                    sub_name_components.append("selection")
                elif "domain" in metadata:
                    sub_name_components.append("domain")
                name_components.append(sub_name_components)
            elif "site" in metadata:
                name_components.append("site")

            name_components.append("platform")

    filename = define_filename(
        name_components,
        metadata=metadata,
        obs_type=obs_type,
        include_version=include_version,
        data_version=data_version,
        output_path=output_path,
    )

    return filename


def find_data_version(metadata: dict) -> Optional[str]:
    """
    Find the latest version from within the metadata by looking for the "latest_version" key.
    Args:
        metadata: Dictionary containing metadata values
    Returns:
        str / None: Version number or None if this is not found.
    """
    version_key = "latest_version"
    return metadata.get(version_key)


def define_obspack_filename(
    metadata: dict,
    obs_type: str,
    obspack_path: pathType,
    include_version: bool = True,
    data_version: Optional[str] = None,
    name_components: Optional[list] = None,
) -> Path:
    """
    Create file name for obspack files with expected naming convention. This will
    depend on the obs_type.

    Args:
        metadata: Dictionary containing metadata values
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        obspack_path: Top level directory for obspack
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. Default will depend on obs_type.

    Returns:
        Path: Full path for filename
    """
    obs_types = define_obs_types()
    if obspack_path is not None:
        obspack_path = Path(obspack_path)

    if obs_type in obs_types:
        if "surface" in obs_type:
            full_obspack_path = Path(obspack_path) / obs_type
            filename = define_surface_filename(
                metadata=metadata,
                obs_type=obs_type,
                output_path=full_obspack_path,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
            )
        elif "column" in obs_type:
            full_obspack_path = Path(obspack_path) / obs_type
            filename = define_column_filename(
                metadata=metadata,
                obs_type=obs_type,
                output_path=full_obspack_path,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
            )
    else:
        raise ValueError(f"Did not recognise obs_type {obs_type}. Should be one of: {obs_types}")

    return filename


def find_current_obspacks(output_folder: pathType, obspack_stub: str) -> list[Path]:
    """
    Find obspacks within the supplied output folder. Expect these will start with
    the obspack_stub string.

    Args:
        output_folder: Path to top level directory where obspack folder will be created.
            When looking for previous obspacks, will look here for these.
        obspack_stub: Start of the obspack_name. This will be assumed to be standard for
            other obspacks of the same type.
    Returns:
        list : Path to current folders identified as current obspacks within output_folder
    """

    output_folder = Path(output_folder)
    current_obspacks = [folder for folder in output_folder.glob(f"{obspack_stub}_*") if folder.is_dir()]
    current_obspacks.sort()

    return current_obspacks


def define_obspack_name(
    obspack_stub: str,
    version: Optional[str] = None,
    major_version_only: bool = False,
    minor_version_only: bool = False,
    output_folder: optionalPathType = None,
    current_obspacks: Optional[list] = None,
) -> tuple[str, str]:
    """
    Define the name of the obspack based on an obspack_stub and version.
    This will be formatted as:
        "{obspack_stub}_{version}"

    If version is not specified the version will be extracted by looking
    for folders following the same naming scheme either using the output_folder
    or a supplied list of current obspack names.

    If the only obspack_stub from previous obspacks has no associated version, this will
    be treated as "v1" and the new obspack name as the next version.

    Args:
        output_folder: Path to top level directory where obspack folder will be created.
            When looking for previous obspacks, will look here for these.
        obspack_stub: Start of the obspack_name. This will be assumed to be standard for
            other obspacks of the same type.
        version: Can explicitly define a version to be used to create the obspack_name
        major_version_only: Only increase the major version.
        minor_version_only: Only increase the minor version.
        current_obspacks: List of previous obspacks to use when defining the new version
            in obspack_name
    Returns:
        (str, str): Obspack name and the version associated with this

    TODO: Decide if to also incorporate rough date of creation
    """

    if version is None:
        if current_obspacks is None:
            if output_folder is not None:
                current_obspacks = find_current_obspacks(output_folder, obspack_stub)
            else:
                logger.exception(
                    "Output_folder must be supplied to define a new obspack name based on obspack_stub.\n"
                    "Alternatively, supply a complete obspack_name rather than an obspack_stub."
                )

        if current_obspacks:
            version_pattern = re.compile(
                rf"{obspack_stub}_?v?(?P<major_version>\d*)[.]?(?P<minor_version>\d*)"
            )
            versions = [(-1, -1)]
            for c_obspack in current_obspacks:
                version_search = version_pattern.search(str(c_obspack))
                if version_search is not None:
                    version_dict = version_search.groupdict()
                    major_version = version_dict["major_version"]
                    minor_version = version_dict["minor_version"]

                    # If obspack stub is found but with no associated version
                    # we should treat this as "v1" and ensure the new obspack is the next version (e.g. "v2" or "v1.1")
                    if major_version == "":
                        major_version = 1
                    if minor_version == "":
                        minor_version = -1

                    version_pairs = (int(major_version), int(minor_version))
                    versions.append(version_pairs)

            max_version = max(versions)

            if max_version[0] == -1:
                if minor_version_only:
                    version = "v1.0"
                else:
                    version = "v1"
            elif max_version[1] == -1:
                if minor_version_only:
                    version = f"v{max_version[0]}.1"
                else:
                    version = f"v{max_version[0] + 1}"
            else:
                if major_version_only:
                    version = f"v{max_version[0] + 1}"
                else:
                    version = f"v{max_version[0]}.{max_version[1] + 1}"
        else:
            if minor_version_only:
                version = "v1.0"
            else:
                version = "v1"

    obspack_name = f"{obspack_stub}_{version}"

    return obspack_name, version


def define_obspack_path(output_folder: pathType, obspack_name: str) -> Path:
    """
    Define the full output path for the obspack folder.
    Args:
        output_folder: Path to top level directory where obspack folder will be created
        obspack_name: Name of obspack to be created
    Returns:
        Path: Full obspack folder name
    """
    return Path(output_folder) / obspack_name


def default_release_files() -> list:
    """
    Release files which will be included in the created obspack by default.
    This will return a list of filepaths to these default files.
    """
    from contextlib import ExitStack

    file_manager = ExitStack()
    release_file_ref = importlib.resources.files("openghg") / "data/obspack/obspack_README.md"
    release_file_path = file_manager.enter_context(importlib.resources.as_file(release_file_ref))

    release_files = [release_file_path]
    return release_files


def create_obspack_structure(
    output_folder: pathType,
    obspack_name: str,
    obs_types: Sequence = define_obs_types(),
    release_files: Optional[Sequence] = None,
) -> Path:
    """
    Create the structure for the new obspack and add initial release files to be included.

    Args:
        output_folder: Path to top level directory where obspack folder will be created
        obspack_name: Name of obspack to be created
        obs_types: Observation types to include in obspack. Sub-folders will be created for these obs_types.
        release_files: Release files to be included within the output obspack.
            - If release_files=None (default) this will use the files defined by default_release_files() function.
            - If release_files=[] no release files will be included in the obspack.
    Returns:
        Path: Path to top level obspack directory {output_folder}/{obspack_name}
    """

    obspack_path = define_obspack_path(output_folder, obspack_name)

    if release_files is None:
        release_files = default_release_files()

    logger.info(f"Creating top level obspack folder: {obspack_path} and subfolder(s)")
    for subfolder in obs_types:
        subfolder = obspack_path / subfolder
        subfolder.mkdir(parents=True)

    for file in release_files:
        shutil.copy(file, obspack_path)

    return obspack_path


def read_input_file(filename: pathType) -> pd.DataFrame:
    """
    Read input file containing search parameters as a pandas DataFrame.

    This should include at least 1 search parameter column and an obs_type column.
    See define_obs_types() for different obs types currently supported.

    Example:
    >    site,inlet,species,obs_type
    >    tac,185m,co2,surface-insitu
    >    bsd,42m,ch4,surface-insitu
    >    bsd,100m-250m,ch4,surface-insitu

    Args:
        filename: Filename containing search parameters and obs_type column.
    Returns:
        pandas.DataFrame: Input file opened as a csv file using pd.read_csv(...)

    TODO: Add checks for expected columns in input search file as required
    """
    search_df = pd.read_csv(filename)
    return search_df


def retrieve_data(
    filename: optionalPathType = None, search_df: Optional[pd.DataFrame] = None, store: Optional[str] = None
) -> list:
    """
    Use search parameters to get data from object store. This expects either a filename for an input
    file containing search parameters (see read_input_file() for more details) or a DataFrame containing
    the search parameters.

    Args:
        filename: Filename containing search parameters.
        search_df: pandas DataFrame containing search parameters
        store: Name of specific object store to use to search for data
    Returns:
        list [ObsSurface/ObsColumn]: List of extracted data from the object store based on search parameters
    """

    if filename:
        search_df = read_input_file(filename)

    if search_df is None:
        raise ValueError("Either filename or extracted search dataframe must be supplied to retrieve data")

    obs_type_name = "obs_type"
    default_obs_type = "surface-insitu"
    if obs_type_name not in search_df.columns:
        logger.warning(
            f"No 'obs_type' column has been supplied within the search parameters. Defaulting to: '{default_obs_type}'"
        )
        search_df[obs_type_name] = default_obs_type

    get_functions, get_fn_arguments = define_get_functions()

    data_object_all = []
    for i, row in search_df.iterrows():
        obs_type = row[obs_type_name]
        get_fn = get_functions[obs_type]
        additional_fn_arguments = get_fn_arguments[get_fn]

        kwargs = row.to_dict()
        kwargs.pop(obs_type_name)

        if store:
            kwargs["store"] = store

        # TODO: Update to a more robust check (may be code to already do this?)
        if "-" in kwargs["inlet"]:
            start, end = kwargs["inlet"].split("-")
            kwargs["inlet"] = slice(start, end)

        # Remove any NaN entries
        # TODO: Decide how this could work with negative lookup?
        kwargs = {key: value for key, value in kwargs.items() if not pd.isnull(value)}

        # Pass any additional arguments needed for the get/search function
        kwargs = kwargs | additional_fn_arguments

        # TODO: Decide details around catching errors for no files/multi files found.
        data_retrieved = get_fn(**kwargs)
        data_object_all.append(data_retrieved)

    return data_object_all


def collate_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce pandas data frame rows by combining unique entries within a column into a single string separated by a semi-colon.
    This can be used as part of applying a function to a split DataFrame (e.g. via groupby)

    Args:
        df: any pandas DataFrame
    Returns:
        pandas.DataFrame: A new, single row DataFrame
    """
    df_new = pd.DataFrame()
    for name, series in df.items():
        unique_values = series.unique()
        collated_value = ",".join([str(value) for value in unique_values])

        df_new[name] = [collated_value]

    return df_new


def define_site_details(ds: xr.Dataset, obs_type: str, strict: bool = False) -> dict:
    """
    Extract associated site details as a dictionary for a given Dataset. Expect these details to
    be included within the dataset attributes.
    This can be used as a way to build up a DataFrame from a set of dictionaries and defines friendly
    column names for this output.

    Overall attributes which this will attempt to extract are:
        - "site"
        - "station_long_name"
        - "inlet"
        - "station_latitude"
        - "station_longitude"
        - "instrument"
        - "network"
        - "data_owner"
        - "data_owner_email"

    Args:
        ds: Expect this dataset to contain useful attributes describing the site data.
        obs_type: Observation type associated with this dataset (see define_obs_types() for full list)
        strict: Whether to raise an error if any key is missing. Default = False
    Returns:
        dict: Dictionary containing extracted site details from Dataset
    """
    attrs = ds.attrs

    key_names = {
        "site": "Site code",
        "station_long_name": "Name",
        "inlet": "Inlet height(s)",
        "station_latitude": "Latitude",
        "station_longitude": "Longitude",
        "instrument": "Instrument",
        "network": "Associated network",
        "data_owner": "Data owner",
        "data_owner_email": "Email",
    }

    params = {}
    for key, new_name in key_names.items():
        if key in attrs:
            params[new_name] = attrs[key]
        else:
            msg = f"Unable to find '{key}' key in site data attributes"
            if strict:
                logger.exception(msg)
                raise ValueError(msg)
            else:
                logger.warning(msg)
                params[new_name] = np.nan

    params["Observation type"] = obs_type

    return params


def create_site_index(df: pd.DataFrame, output_filename: pathType) -> None:
    """
    Creates the site index file including data provider details.
    Args:
        df : DataFrame containing the collated details for the site from the associated metadata.
        output_filename : Filename to use for writing site details.
    Returns:
        None
    """

    # Site index should include key metadata for each file (enough to be distinguishable but not too much?)
    # Want to create a DataFrame and pass a file object to this - then can add comments before and after the table as needed

    site_column = "Site code"

    if site_column not in df.columns:
        msg = "Unable to create site details file: 'Site code' column is not present in provided DataFrame."
        logger.exception(msg)
        raise ValueError(msg)

    logger.info(f"Writing site details to: {output_filename}")
    output_file = open(output_filename, "w")

    site_details = df.groupby(site_column).apply(collate_strings, include_groups=False)
    site_details = site_details.dropna(axis=1, how="all")

    # index_output_name = open(obspack_folder / f"site_index_details_{version}.txt", "w")

    # Add header to file
    output_file.write("++++++++++++\n")
    output_file.write("Site details\n")
    output_file.write(f"File created: {pd.to_datetime('today').normalize()}\n")
    output_file.write("++++++++++++\n")
    output_file.write("\n")

    site_details.to_csv(output_file, index=False, sep="\t")

    output_file.close()


def create_obspack(
    search_filename: optionalPathType = None,
    search_df: Optional[pd.DataFrame] = None,
    output_folder: pathType = pathlib.Path.home(),
    obspack_name: Optional[str] = None,
    obspack_stub: Optional[str] = None,
    version: Optional[str] = None,
    major_version_only: bool = False,
    minor_version_only: bool = False,
    current_obspacks: Optional[list] = None,
    release_files: Optional[Sequence] = None,
    include_data_versions: bool = True,
    store: Optional[str] = None,
) -> Path:
    """
    Create ObsPack from observation stored within an object store based on input search parameters.

    Args:
        search_filename: Filename of the search parameters as a csv file. Expect this to contain an 'obs_type' column with details
            of the observation type for the data. See define_obs_types() for list of inputs.
        search_df: DataFrame equivalent of the search_filename. This or search_filename MUST be supplied.
        output_folder: Top level directory for the obspack to be written to disc.
            Default: user home directory.
        obspack_name: Full name for the obspack
        obspack_stub: As an alternative to the full obspack_name, an obspack_stub can be specified which will have a version.
            See define_obspack_name() function for details of how the name is constructed when an obspack_stub is specified.
        version: Version to include with an obspack_stub. If not specified, this will be detected.
        minor_version_only: When automatically checking for versions, only the minor version will be iterated (e.g. 2.0 --> 2.1)
        major_version_only: When automatically checking for versions, only the major version will be iterated (e.g. 2.0 --> 3.0)
        current_obspacks: List of previous obspacks to use when defining the new version
            in obspack_name.
        release_files: Additional release files to include within the obspack. See default_release_files() for details of what files
            will be included by default.
        include_data_versions: Whether to include the internal data versions for the stored data. Default = True.
        store: Name of the object store to use to extract the data.
    Returns:
        Path : Path to created obspack
    """

    if search_df is None:
        if search_filename is not None:
            search_df = read_input_file(search_filename)
        else:
            msg = "Either search_filename or search_df must be specified to create an obspack."
            logger.exception(msg)
            raise ValueError(msg)

    retrieved_data = retrieve_data(search_df=search_df, store=store)

    obs_types = search_df["obs_type"]
    unique_obs_types = np.unique(obs_types)

    if obspack_name is None and obspack_stub:
        obspack_name, version = define_obspack_name(
            obspack_stub,
            version=version,
            major_version_only=major_version_only,
            minor_version_only=minor_version_only,
            current_obspacks=current_obspacks,
            output_folder=output_folder,
        )
    elif version is None:
        version = "v1"

    obspack_name = cast(str, obspack_name)

    obspack_path = create_obspack_structure(
        output_folder, obspack_name, obs_types=unique_obs_types, release_files=release_files
    )

    site_detail_rows = []
    for data, obs_type in zip(retrieved_data, obs_types):
        metadata = data.metadata

        output_name = define_obspack_filename(
            metadata=metadata,
            obs_type=obs_type,
            obspack_path=obspack_path,
            include_version=include_data_versions,
        )
        ds = data.data
        ds.to_netcdf(output_name)

        site_details = define_site_details(ds, obs_type)
        site_detail_rows.append(site_details)

    index_output_filename = obspack_path / f"site_index_details_{version}.txt"

    site_details_df = pd.DataFrame(site_detail_rows)
    create_site_index(site_details_df, index_output_filename)

    return obspack_path
