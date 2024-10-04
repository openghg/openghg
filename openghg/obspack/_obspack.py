import pandas as pd
import xarray as xr
import shutil
from pathlib import Path
import pkg_resources
from typing import Union, Optional, Sequence
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


def define_get_functions() -> dict:
    """
    Define relationship between obs type names (for folders) and get functions.
    """
    get_functions = {
        "surface-insitu": get_obs_surface,
        "surface-flask": get_obs_surface,
        "column": get_obs_column,
    }

    return get_functions


def define_filename(
    name_components: list[Union[str, list]],
    metadata: dict,
    obs_type: Optional[str] = None,
    version: str = "v1",
    output_path: optionalPathType = None,
) -> Path:
    """
    Define filename based on determined structure. The input name_components determines
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
        version: Version number for obspack. Default = "v1"
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

        full_name = "-".join(sub_name_details)
        name_details.append(full_name)

    if missing_keys:
        msg = f"Necessary keys missing from stored metadata: {', '.join(missing_keys)}"
        logger.exception(msg)
        raise ValueError(msg)

    if obs_type is not None:
        filename = Path(f"{'_'.join(name_details)}_{obs_type}_{version}.nc")
    else:
        filename = Path(f"{'_'.join(name_details)}_{version}.nc")

    if output_path is not None:
        filename = output_path / filename

    return filename


def define_surface_filename(
    metadata: dict,
    obs_type: Optional[str] = None,
    version: str = "v1",
    output_path: optionalPathType = None,
) -> Path:
    """
    Create file name for surface type (surface-flask or surface-insitu)
    data with expected naming convention.

    Args:
        metadata: Dictionary containing metadata values. Expect at least "site",
            "species" and "inlet" to be present.
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        version: Version number for obspack. Default = "v1"
        output_path: Full output folder path for file
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into naming?
    """
    name_components: list[Union[str, list]] = ["species", "site", "inlet"]
    filename = define_filename(
        name_components, metadata=metadata, obs_type=obs_type, output_path=output_path, version=version
    )
    return filename


def define_column_filename(
    metadata: dict,
    obs_type: str = "column",
    version: str = "v1",
    output_path: optionalPathType = None,
) -> Path:
    """
    Create file name for column type data with expected naming convention.

    Args:
        metadata: Dictionary containing metadata values. Expect at least "platform"
            and "species" to be present.
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        version: Version number for obspack. Default = "v1"
        output_path: Full output folder path for file
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into naming?
    """

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
        output_path=output_path,
        version=version,
    )

    return filename


def define_obspack_filename(
    metadata: dict,
    obs_type: str,
    obspack_path: pathType,
    version: str = "v1",
) -> Path:
    """
    Create file name for obspack files with expected naming convention. This will
    depend on the obs_type.

    Args:
        metadata: Dictionary containing metadata values
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        obspack_path: Top level directory for obspack
        version: Version number for obspack. Default = "v1"
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
                metadata=metadata, obs_type=obs_type, version=version, output_path=full_obspack_path
            )
        elif "column" in obs_type:
            full_obspack_path = Path(obspack_path) / obs_type
            filename = define_column_filename(
                metadata=metadata, obs_type=obs_type, version=version, output_path=full_obspack_path
            )
    else:
        raise ValueError(f"Did not recognise obs_type {obs_type}. Should be one of: {obs_types}")

    return filename


def define_obspack_path(output_folder: pathType, obspack_name: str) -> Path:
    """
    Define the full output path for the obspack folder.
    """
    return Path(output_folder) / obspack_name


def create_obspack_structure(
    output_folder: pathType,
    obspack_name: str,
    obs_types: Sequence = define_obs_types(),
    release_files: Optional[Sequence] = None,
) -> Path:
    """
    Create the structure for the new obspack
    """

    obspack_path = define_obspack_path(output_folder, obspack_name)

    if release_files is None:
        release_file_readme = pkg_resources.resource_filename("openghg", "data/obspack/obspack_README.md")
        release_files = [release_file_readme]

    logger.info(f"Creating top level obspack folder: {obspack_path} and subfolder(s)")
    for subfolder in obs_types:
        subfolder = obspack_path / subfolder
        subfolder.mkdir(parents=True)

    for file in release_files:
        shutil.copy(file, obspack_path)

    return obspack_path


def read_input_file(filename: pathType) -> tuple[list, list]:
    """
    Read input file containing search parameters and use to get data from object store.
    """
    search_df = pd.read_csv(filename)

    obs_type_name = "obs_type"
    get_functions = define_get_functions()

    data_object_all = []
    unique_obs_types = []
    for i, row in search_df.iterrows():
        obs_type = row[obs_type_name]
        get_fn = get_functions[obs_type]

        kwargs = row.to_dict()
        kwargs.pop(obs_type_name)

        # TODO: Update to a more robust check (may be code to already do this?)
        if "-" in kwargs["inlet"]:
            start, end = kwargs["inlet"].split("-")
            kwargs["inlet"] = slice(start, end)

        # TODO: Decide details around catching errors for no files/multi files found.
        surface_data = get_fn(**kwargs)
        # search_results = search_surface(**kwargs)
        # data_retrieved = search_results.retrieve()
        data_object_all.append(surface_data)

        if obs_type not in unique_obs_types:
            unique_obs_types.append(obs_type)

    return data_object_all, unique_obs_types


def collate_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each unique entry in a column, collate this as a single string separated by a semi-colon.
    """
    df_new = pd.DataFrame()
    for name, series in df.items():
        unique_values = series.unique()
        collated_value = ",".join([str(value) for value in unique_values])

        df_new[name] = [collated_value]

    return df_new


def define_site_details(ds: xr.Dataset, obs_type: str, strict: bool = False) -> dict:
    """
    Define each row containing the site details.
    """
    attrs = ds.attrs

    key_names = {
        "site": "Site code",
        "station_long_name": "Name",
        "inlet": "Inlet height",
        "station_latitude": "Latitude",
        "station_longitude": "Longitude",
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

    params["Observation type"] = obs_type

    return params


def create_site_index(df: pd.DataFrame, output_folder: pathType) -> None:
    """
    Creates the site index file including data provider details.
    Expects a DataFrame object.
    """

    logger.info(f"Writing site details: {output_folder}")
    output_file = open(output_folder, "w")

    # Site index should include key metadata for each file (enough to be distinguishable but not too much?)
    # Want to create a DataFrame and pass a file object to this - then can add comments before and after the table as needed

    site_details = df.groupby("Site code").apply(collate_strings)
    site_details = site_details.rename(columns={"Inlet height": "Inlet heights"})

    # index_output_name = open(obspack_folder / f"site_index_details_{version}.txt", "w")

    # Add header to file
    output_file.write("++++++++++++\n")
    output_file.write("Site details\n")
    output_file.write("++++++++++++\n")
    output_file.write("\n")

    site_details.to_csv(output_file, index=False, sep="\t")

    output_file.close()


def create_obspack(
    filename: pathType,
    output_folder: pathType,
    obspack_name: str,
    release_files: Optional[Sequence] = None,
) -> None:
    """
    Create ObsPack of obspack_name at output_folder from input search file.
    """

    retrieved_data, obs_types = read_input_file(filename)

    obspack_path = create_obspack_structure(
        output_folder, obspack_name, obs_types=obs_types, release_files=release_files
    )

    # Put things in obspack and build structure
    # TODO: TEMPORARY - To be updated and generalised
    version = "v1"
    obs_type = "surface-insitu"
    ####

    site_detail_rows = []
    for data in retrieved_data:
        metadata = data.metadata

        output_name = define_obspack_filename(
            metadata=metadata, obs_type=obs_type, obspack_path=obspack_path, version=version
        )
        ds = data.data
        ds.to_netcdf(output_name)

        site_details = define_site_details(ds, obs_type)
        site_detail_rows.append(site_details)

    index_output_filename = obspack_path / f"site_index_details_{version}.txt"

    site_details_df = pd.DataFrame(site_detail_rows)
    create_site_index(site_details_df, index_output_filename)
