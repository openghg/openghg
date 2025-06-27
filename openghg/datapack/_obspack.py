"""
This module is allow the creation of an output data obspack based on files stored within the object store.

The default structure is to create a top level directory (obspack_name) and for this to contain subfolders for each of the obs_types.
The output files will be in netcdf format and will be based on a naming convention dependent on the obs_type.

The default obspack will also contain:
 - obspack_README.md - general details about use of an obspack
 - site_index_details*.txt - constructed site details based on contents of the obspack

Default overall obspack structure:
{obspack_name}/
    obspack_README.md
    site_index_details*.txt
    surface-insitu/
        {species}_{site}_{inlet}_surface-insitu_{data_version}.nc
    ...

Key functions:
 - define_obspack_filename() - defines the full output filename for each file based on naming convention
 - retrieve_data() - retrieve data from an object store search terms (currently from a config file)
 - create_obspack() - this is the summary function for creating an obspack
"""

import re
import numpy as np
import pandas as pd
import xarray as xr
import shutil
import pathlib
from pathlib import Path

import importlib.resources
from typing import Sequence, cast
import logging

from openghg.dataobjects import ObsData, ObsColumnData
from openghg.retrieve import get_obs_surface, get_obs_column
from openghg.types import pathType

logger = logging.getLogger("openghg.obspack")

# TODO: Move to types submodule?
ObsOutputType = ObsData | ObsColumnData
NameComponents = list[str | list]
MultiNameComponents = dict[str, NameComponents] | NameComponents | None
MultiSubFolder = dict[str, pathType] | pathType | None


class StoredData:
    """
    This class contains details of the data extracted from the object store with details
    of how this is related to the obspack this will be outputted to.
    """

    def __init__(
        self,
        data: ObsOutputType,
        obs_type: str = "surface-insitu",
        obspack_path: pathType | None = None,
        obspack_filename: pathType | None = None,
        subfolder: MultiSubFolder = None,
        data_version: str | None = None,
    ):
        """
        Creation of a StoredData object. This expects a retrieved data object from the object store.

        Note: at the moment this is specific to observation types but this could be expanded
        to include all output data types
        """
        self.stored_data = data
        self.data = data.data
        self.metadata = data.metadata

        self.obs_type = obs_type
        self.obspack_path = obspack_path

        self.obspack_filename = Path(obspack_filename) if obspack_filename is not None else None

        if isinstance(subfolder, dict):
            subfolder = subfolder[self.obs_type]
        self.subfolder = subfolder

        if data_version is None:
            data_version = find_data_version(self.metadata)
        self.data_version = data_version

    def make_obspack_filename(
        self,
        obspack_path: pathType | None = None,
        subfolder: MultiSubFolder = None,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents = None,
        name_suffixes: dict | None = None,
    ) -> Path:

        # Update attributes on the object if specified
        self.obspack_path = obspack_path if obspack_path is not None else self.obspack_path
        self.data_version = data_version if data_version is not None else self.data_version

        if isinstance(subfolder, dict):
            subfolder = subfolder[self.obs_type]
        self.subfolder = subfolder if subfolder is not None else self.subfolder

        obspack_filename = define_obspack_filename(
            self.metadata,
            self.obs_type,
            obspack_path=self.obspack_path,
            subfolder=self.subfolder,
            include_obs_type=include_obs_type,
            include_version=include_version,
            data_version=self.data_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )

        return obspack_filename

    def update_obspack_filename(
        self,
        obspack_path: pathType | None = None,
        subfolder: MultiSubFolder = None,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents = None,
        name_suffixes: dict | None = None,
    ) -> Path:

        obspack_filename = self.make_obspack_filename(
            obspack_path=obspack_path,
            subfolder=subfolder,
            include_obs_type=include_obs_type,
            include_version=include_version,
            data_version=data_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )

        self.obspack_filename = obspack_filename

        return obspack_filename


def define_obs_types() -> list:
    """
    Define names of obs type folders to be included in an ObsPack.
    """
    obs_types = ["surface-insitu", "surface-flask", "column"]
    return obs_types


def define_data_type_for_obs_type(obs_type: str) -> str:
    """
    Define associated data_type within the object store for a given obs_type.
    """
    # TODO: Decide if we should update column to surface-column and satellite
    # etc. - do we want to link this to "platform"?
    data_types = {"surface-insitu": "surface", "surface-flask": "surface", "column": "column"}

    data_type = data_types[obs_type]

    return data_type


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


def _construct_name(keys: list | str, dictionary: dict, separators: Sequence[str]) -> str:
    """
    Recursive function to create a composite string by selecting values from a dictionary
    using supplied keys and appropriate separators.

    This allows keys to be nested to make this use different separators for different key selections.
    """
    if isinstance(keys, list):
        try:
            # Attempt to grab the head (element 0) and tail (elements 1:) from the Sequence
            separator, *separators = separators
        except ValueError as exc:
            raise ValueError("When constructing names, separators must be >= depth of keys input.\n") from exc

        # Generator expression to loop over elements
        values = (_construct_name(key, dictionary, separators) for key in keys)
        return separator.join(values)
    else:
        try:
            value = str(dictionary[keys])
        except KeyError:
            raise ValueError(
                f"When constructing names, unable to find key: '{keys}' within supplied dictionary"
            )

        return value


def define_filename(
    name_components: list,
    metadata: dict,
    name_suffixes: dict,
    output_path: pathType | None = None,
    separators: str | tuple = ("_", "-"),
) -> Path:
    """
    Define filename based on the determined structure. The input name_components determines
    the initial naming strings, extracting these from the metadata.
    Sub-names can be specified within name_components by including these as a list.

    By default, the name_components inputs will be interpreted as follows:
     - list elements (names) will be separated by underscores "_"
     - any sub-list elements (sub-names) will be seperated by dashes "-"

    Args:
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename.
        metadata: Dictionary containing the metadata keys to use
        name_suffixes: Dictionary of additional values to add to the filename as a suffix.
        output_path: Full output folder path for file
        separators: Seperators to use between name_components. Default = ("_", "-")
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

    file_extension = ".nc"

    if output_path is not None:
        output_path = Path(output_path)

    if isinstance(separators, str):
        separators = (separators,)

    filename_stub = _construct_name(name_components, metadata, separators)

    suffixes = list(name_suffixes.values())

    all_components = [filename_stub] + suffixes
    filename_stub = separators[0].join(all_components)

    filename = Path(f"{filename_stub}{file_extension}")

    if output_path is not None:
        filename = Path(output_path) / filename

    return filename


def define_name_suffixes(
    obs_type: str | None = None,
    include_obs_type: bool = True,
    data_version: str | None = None,
    include_version: bool = True,
) -> dict:
    """
    Define default suffix dictionary for output filename.
    Dependent on the `include_obs_type` and `include_version` flags this will
    typically include obs_type and latest version.

    Args:
        obs_type: Name of the observation type associated with the data.
        include_obs_type: Whether to include obs_type as the first suffix.
        data_version: Version of the data.
        include_version: Whether to include version as as the last suffix.
    Returns:
        dict: Dictionary of suffix names and values
    """
    name_suffixes = {}
    if include_obs_type:
        if obs_type is not None:
            name_suffixes["obs_type"] = obs_type
    if include_version:
        if data_version is not None:
            name_suffixes["data_version"] = data_version

    return name_suffixes


def define_name_components(obs_type: str, metadata: dict | None = None) -> NameComponents:
    """
    Define the default naming scheme for the input obs_type.

    Args:
        obs_type: Name of the observation type associated with the data.
        metadata: Only needed if obs_type="column". This is because the platform from the
            the metadata is used to determine whether the site or satellite naming scheme
            should be used.
    Returns:
        list: Keys to use when extracting names from the metadata and to use within the filename.
    """
    if "surface" in obs_type:
        name_components: NameComponents = ["species", "site", "inlet"]
    elif "column" in obs_type:
        if metadata is None:
            raise ValueError(
                "To define the filename commponents for 'column' data, metadata must be supplied."
            )

        try:
            platform = metadata["platform"]
        except KeyError:
            msg = "Expect 'platform' key to be included for 'column' data. Please check metadata"
            logger.exception(msg)
            raise ValueError(msg)

        if platform == "site":
            name_components = ["species", "site", "platform"]
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

    return name_components


def define_surface_filename(
    metadata: dict,
    obs_type: str | None = None,
    output_path: pathType | None = None,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: list | None = None,
    name_suffixes: dict | None = None,
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
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. Using the default sets this to ["species", "site", "inlet"]
        name_suffixes: Dictionary of additional values to add to the filename as a suffix.
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into file naming?
    """

    if name_components is None:
        if obs_type is not None:
            name_components = define_name_components(obs_type=obs_type)
        else:
            raise ValueError(
                "Must specify name_components directly or obs_type so default name_components can be defined."
            )

    if data_version is None:
        data_version = find_data_version(metadata)

    if name_suffixes is None:
        name_suffixes = define_name_suffixes(
            obs_type=obs_type,
            include_obs_type=include_obs_type,
            data_version=data_version,
            include_version=include_version,
        )

    filename = define_filename(
        name_components,
        metadata=metadata,
        name_suffixes=name_suffixes,
        output_path=output_path,
    )

    return filename


def define_column_filename(
    metadata: dict,
    obs_type: str = "column",
    output_path: pathType | None = None,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: list | None = None,
    name_suffixes: dict | None = None,
) -> Path:
    """
    Create file name for column type data with expected naming convention.

    Args:
        metadata: Dictionary containing metadata values. Expect at least "platform"
            and "species" to be present.
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        output_path: Full output folder path for file
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. Using the default uses the platform to determine the naming.
        name_suffixes: Dictionary of additional values to add to the filename as a suffix.
    Returns:
        Path: Full path for filename

    TODO: Would we want to incorporate instrument into naming?
    """

    if name_components is None:
        name_components = define_name_components(obs_type=obs_type, metadata=metadata)

    if data_version is None:
        data_version = find_data_version(metadata)

    if name_suffixes is None:
        name_suffixes = define_name_suffixes(
            obs_type=obs_type,
            include_obs_type=include_obs_type,
            data_version=data_version,
            include_version=include_version,
        )

    filename = define_filename(
        name_components,
        metadata=metadata,
        name_suffixes=name_suffixes,
        output_path=output_path,
    )

    return filename


def find_data_version(metadata: dict) -> str | None:
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
    obspack_path: pathType | None = None,
    subfolder: MultiSubFolder = None,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: MultiNameComponents = None,
    name_suffixes: dict | None = None,
) -> Path:
    """
    Create file name for obspack files with expected naming convention. This will
    depend on the obs_type.

    Args:
        metadata: Dictionary containing metadata values
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
        obspack_path: Top level directory for obspack
        subfolder: By default the obs_type will be used to create a subfolder. Specifying a subfolder directly
            supercedes this. Pass an empty string for no subfolder.
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. This can be specified per obs_type using a dictionary and the obs_type
            input will be used to select the appropriate values.
        name_suffixes: Dictionary of additional values to add to the filename as a suffix.
    Returns:
        Path: Full path for filename
    """
    obs_types = define_obs_types()
    if obspack_path is not None:
        obspack_path = Path(obspack_path)

    if obs_type in obs_types:

        if isinstance(subfolder, dict):
            try:
                subfolder = Path(subfolder[obs_type])
            except KeyError:
                raise ValueError(
                    f"If subfolder is specified as a dict this should use the obs_type values for the keys. Currently: {list(subfolder.keys())}"  # type:ignore
                )

        if subfolder is None:
            subfolder = obs_type
        else:
            subfolder = Path(subfolder)

        if obspack_path is not None:
            full_obspack_path = Path(obspack_path) / subfolder
        else:
            full_obspack_path = Path(subfolder)

        if isinstance(name_components, dict):
            try:
                name_components = name_components[obs_type]
            except KeyError:
                raise ValueError(
                    f"If name_components is specified as a dict this should use the obs_type values for the keys. Currently: {list(name_components.keys())}"  # type:ignore
                )

        if "surface" in obs_type:
            filename = define_surface_filename(
                metadata=metadata,
                obs_type=obs_type,
                output_path=full_obspack_path,
                include_obs_type=include_obs_type,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
                name_suffixes=name_suffixes,
            )
        elif "column" in obs_type:
            filename = define_column_filename(
                metadata=metadata,
                obs_type=obs_type,
                output_path=full_obspack_path,
                include_obs_type=include_obs_type,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
                name_suffixes=name_suffixes,
            )
    else:
        raise ValueError(f"Did not recognise obs_type {obs_type}. Should be one of: {obs_types}")

    return filename


def check_unique(values: Sequence) -> bool:
    """
    Check whether sequence is unique. Returns True/False.
    """

    return len(values) == len(set(values))


def find_repeats(values: Sequence) -> list[np.ndarray] | None:
    """
    Find repeated indices from within a sequence.
    Returns:
        list[numpy.ndarray]: Grouped arrays containing the repeated indices.
    """

    unique_values, indices, counts = np.unique(values, return_inverse=True, return_counts=True)

    if len(unique_values) == len(values):
        return None

    repeated_indices = np.where(counts > 1)[0]
    repeated_org_indices = [np.where(indices == repeat)[0] for repeat in repeated_indices]

    return repeated_org_indices


def define_obspack_filenames(
    retrieved_data: list[StoredData],
    obspack_path: pathType | None = None,
    subfolders: MultiSubFolder = None,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: MultiNameComponents = None,
    name_suffixes: dict | None = None,
    force: bool = False,
) -> list[Path]:
    """
    Define the obspack_filename values for multiple StoredData objects.
    If the obspack_filename is already present, this will not update by default and will
    use the stored value.

    Args:
        retrieved_data: List of StoredData objects.
        obspack_path: Top level directory for obspack
        subfolders: By default the obs_types will be used to create a subfolder structure. Specifying subfolders directly, supercedes
            this. This can be specified as:
             - no subfolder(s) - pass empty string
             - one subfolder for all files
             - dictionary of subfolders per obs_type.
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. This can be specified per obs_type using a dictionary.
            Default will depend on obs_type - see define_name_components().
        name_suffixes: Dictionary of additional values to add to the filename as a suffix.
        force: Force update of the obspack_filename and recreate this.
    Returns:
        list[pathlib.Path]: Sequence of filenames associated with the files
    """
    filenames = []
    for data in retrieved_data:
        filename = data.obspack_filename
        if filename is None or force:
            filename = data.update_obspack_filename(
                obspack_path=obspack_path,
                subfolder=subfolders,
                include_obs_type=include_obs_type,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
                name_suffixes=name_suffixes,
            )
        filenames.append(filename)

    return filenames


def check_unique_filenames(
    retrieved_data: list[StoredData],
    obspack_path: pathType | None = None,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: list | None = None,
    force: bool = False,
) -> list[list[StoredData]]:
    """
    Check whether filenames associated with retrieved data are unique.

    Args:
        retrieved_data: List of StoredData objects.
        obspack_path: Top level directory for obspack
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename.
        add_to_objects: Add the filename to each of the StoredData objects.
        force: Force update of the obspack_filename and recreate this.
    Returns:
        list: Groups of StoredData objects with have overlapping obspack_filenames
    """

    filenames = define_obspack_filenames(
        retrieved_data,
        obspack_path=obspack_path,
        include_version=include_version,
        data_version=data_version,
        name_components=name_components,
        force=force,
    )

    repeated_indices = find_repeats(filenames)

    data_grouped_repeats: list[list] = []
    if repeated_indices:
        data_grouped_repeats = [
            [retrieved_data[index] for index in index_set] for index_set in repeated_indices
        ]

    return data_grouped_repeats


def _find_additional_metakeys(
    obs_type: str,
    metadata: dict | None = None,
    name_components: list | None = None,
    store: str | None = None,
) -> list:
    """
    From the openghg config for each data_type, find additional metakeys to use when
    defining the output filename. This will remove metadata keys specified within name_components.

    Args:
        obs_type: Name of the observation type.
            See define_obs_types() for details of obs_type values.
        metadata: Metadata details associated with the stored data.
        name_components: Current metadata keys used to define an obspack_filename.
        store: Name of the object store to use when finding additional metakeys.
    Returns:
        list: Additional metakeys defined within the object store
    """

    from openghg.store import get_metakeys

    # TODO: How do we find out which bucket/store is being used? Is there a default if this is not specified?
    if store is None:
        store = "user"
    full_metakeys = get_metakeys(store)

    # Check and extract current name_components being used for filename
    if name_components is None:
        name_components = define_name_components(obs_type, metadata)

    # Flatten any hierarchy in the name_components so we just have the key names
    name_components_flat = []
    for component in name_components:
        if isinstance(component, str):
            name_components_flat.append(component)
        elif isinstance(component, list):
            name_components_flat.extend(component)

    # Extract name of data_type so we can find appropriate metakeys
    # Note: obs_type is getting very similiar to platform - could we link these..?
    data_type = define_data_type_for_obs_type(obs_type)

    # Extract metakeys for data_type and remove any current keys already being used
    key_types = ["required", "optional"]
    metakeys = []
    for key_type in key_types:
        metakeys_data_type = full_metakeys[data_type][key_type]
        metakeys.extend(list(metakeys_data_type))

    for key in name_components_flat:
        if key in metakeys:
            metakeys.remove(key)

    if len(metakeys) == 0:
        raise ValueError("Unable to find additional name components to create unique name.")

    return metakeys


def add_obspack_filenames(
    retrieved_data: list[StoredData],
    obspack_path: pathType | None = None,
    subfolders: MultiSubFolder = None,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: MultiNameComponents = None,
    store: str | None = None,
) -> list[StoredData]:
    """
    Add obspack filenames to StoredData objects in retrieved_data. This is based
    on the metadata associated with the retrieved data.
    If any filenames within the retrieved_data list are not unique, update the filename
    using more keywords within the metadata.

    Note: updates the obspack_filename attributes in place within StoredData objects.

    Args:
        retrieved_data: List of StoredData objects.
        obspack_path: Top level directory for obspack
        subfolders: By default the obs_types will be used to create a subfolder structure. Specifying subfolders directly, supercedes
            this. This can be specified as:
             - no subfolder(s) - pass empty string
             - one subfolder for all files
             - dictionary of subfolders per obs_type.
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_version: Whether to include the data version in the filename. Default = True.
        data_version: Version of the data. If not specified and include_version is True this
            will attempt to extract the latest version details from the metadata.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. This can be specified per obs_type using a dictionary.
            Default will depend on obs_type - see define_name_components().
        store: Name of the object store to use if we need to find the config file for the data type.
            This will be used if names are not unique to work out which keys to create a more
            descriptive filename.
    Returns:
        list: Same list of StoredData objects passed to the function with filenames added.
    """

    # Create default obspack names
    define_obspack_filenames(
        retrieved_data,
        obspack_path=obspack_path,
        subfolders=subfolders,
        include_obs_type=include_obs_type,
        include_version=include_version,
        data_version=data_version,
        name_components=name_components,
        force=True,
    )

    # Check for repeats and update names
    data_grouped_repeats = check_unique_filenames(retrieved_data)
    if data_grouped_repeats:
        for data_group in data_grouped_repeats:

            example_data = data_group[0]
            example_metadata = example_data.metadata
            obs_type = example_data.obs_type

            if name_components is None:
                name_components = define_name_components(obs_type, example_metadata)
            elif isinstance(name_components, dict):
                try:
                    name_components = name_components[obs_type]
                except KeyError:
                    raise ValueError(
                        f"If name_components is specified as a dict this should use the obs_type values for the keys. Currently: {list(name_components.keys())}"  # type:ignore
                    )

            metakeys = _find_additional_metakeys(
                obs_type,
                metadata=example_metadata,
                name_components=name_components,
                store=store,
            )

            filenames = [data.obspack_filename for data in data_group]
            for additional_key in metakeys:
                if check_unique(filenames):
                    break
                else:
                    key_present = [additional_key in data.metadata for data in data_group]
                    if all(key_present):
                        name_components = name_components + [additional_key]

                        logger.info(
                            f"Checking alternative name for non-unique filename: {data_group[0].obspack_filename} with {additional_key}."
                        )
                        filenames = [
                            data.make_obspack_filename(name_components=name_components, subfolder=subfolders)
                            for data in data_group
                        ]
            else:
                raise ValueError(
                    f"Unable to find unique name for {data_group}. Please specify name_components to use."
                )

            # Once unique names have been found add them to the data entries.
            # **Better way to do this?
            for data, filename in zip(data_group, filenames):
                data.obspack_filename = filename

    return retrieved_data


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
    version: str | None = None,
    major_version_only: bool = False,
    minor_version_only: bool = False,
    output_folder: pathType | None = None,
    current_obspacks: list | None = None,
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
    release_file_ref = importlib.resources.files("openghg") / "data/obspack/obspack_README.md"

    with importlib.resources.as_file(release_file_ref) as f:
        release_file_path = f

    release_files = [release_file_path]
    return release_files


def create_obspack_structure(
    output_folder: pathType,
    obspack_name: str,
    obs_types: Sequence = define_obs_types(),
    subfolder_names: Sequence | None = None,
    release_files: Sequence | None = None,
) -> Path:
    """
    Create the structure for the new obspack and add initial release files to be included.

    Args:
        output_folder: Path to top level directory where obspack folder will be created
        obspack_name: Name of obspack to be created
        obs_types: Observation types to include in obspack. By default, sub-folders will be created for these obs_types.
        subfolder_names: Alternatively, can specify a list of subfolders to create directly. This will supercede obs_types input.
        release_files: Release files to be included within the output obspack.
            - If release_files=None (default) this will use the files defined by default_release_files() function.
            - If release_files=[] no release files will be included in the obspack.
    Returns:
        Path: Path to top level obspack directory {output_folder}/{obspack_name}
    """

    obspack_path = define_obspack_path(output_folder, obspack_name)

    if release_files is None:
        release_files = default_release_files()

    if subfolder_names is None:
        subfolder_names = obs_types

    logger.info(f"Creating top level obspack folder: {obspack_path} and subfolder(s): {subfolder_names}")
    for subfolder in subfolder_names:
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
    filename: pathType | None = None, search_df: pd.DataFrame | None = None, store: str | None = None
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
        list [StoredData]: List of extracted data from the object store based on search parameters
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

        stored_data = StoredData(data_retrieved, obs_type=obs_type)

        data_object_all.append(stored_data)

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
    search_filename: pathType | None = None,
    search_df: pd.DataFrame | None = None,
    output_folder: pathType = pathlib.Path.home(),
    obspack_name: str | None = None,
    obspack_stub: str | None = None,
    version: str | None = None,
    major_version_only: bool = False,
    minor_version_only: bool = False,
    current_obspacks: list | None = None,
    release_files: Sequence | None = None,
    subfolders: MultiSubFolder = None,
    include_obs_type: bool = True,
    include_data_versions: bool = True,
    name_components: MultiNameComponents = None,
    store: str | None = None,
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
        subfolders: By default the obs_types will be used to create a subfolder structure. Specifying subfolders directly, supercedes
            this. This can be specified as:
             - no subfolder(s) - pass empty string
             - one subfolder for all files
             - dictionary of subfolders per obs_type.
        include_obs_type: Whether to include obs_type in the filename. Default = True.
        include_data_versions: Whether to include the internal data versions for the stored data. Default = True.
        name_components: Keys to use when extracting names from the metadata and to use
            within the filename. This can be specified per obs_type using a dictionary.
            Default will depend on obs_type - see define_name_components().
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

    unique_obs_types = search_df["obs_type"].unique()

    if obspack_name is None:
        if obspack_stub:
            obspack_name, version = define_obspack_name(
                obspack_stub,
                version=version,
                major_version_only=major_version_only,
                minor_version_only=minor_version_only,
                current_obspacks=current_obspacks,
                output_folder=output_folder,
            )
        else:
            msg = "Either obspack_name or obspack_stub must be specified when creating an obspack."
            logger.exception(msg)
            raise ValueError(msg)
    elif version is None:
        version = "v1"

    obspack_name = cast(str, obspack_name)

    if isinstance(subfolders, dict):
        subfolder_names = list(subfolders.values())
    elif isinstance(subfolders, str):
        subfolder_names = [subfolders]
    else:
        subfolder_names = None

    obspack_path = create_obspack_structure(
        output_folder,
        obspack_name,
        obs_types=unique_obs_types,
        subfolder_names=subfolder_names,
        release_files=release_files,
    )

    # Create default obspack filenames for data
    # If any duplicates are found and update to use more of the metadata be more specific
    retrieved_data = add_obspack_filenames(
        retrieved_data,
        obspack_path=obspack_path,
        subfolders=subfolders,
        include_obs_type=include_obs_type,
        include_version=include_data_versions,
        name_components=name_components,
    )

    site_detail_rows = []
    for data in retrieved_data:
        ds = data.data
        ds.to_netcdf(data.obspack_filename)

        site_details = define_site_details(ds, data.obs_type)
        site_detail_rows.append(site_details)

    index_output_filename = obspack_path / f"site_index_details_{version}.txt"

    site_details_df = pd.DataFrame(site_detail_rows)
    create_site_index(site_details_df, index_output_filename)

    return obspack_path
