import re
from pathlib import Path
from typing import Sequence
import logging

from openghg.types import pathType

from ._specification import define_obs_types, define_data_type_for_obs_type

logger = logging.getLogger("openghg.obspack")

# Define types related to filename and path creation
NameComponents = list[str | list]

MultiNameComponents = dict[str, NameComponents] | NameComponents
MultiSubFolder = dict[str, pathType] | pathType


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
        separators: Seperators to use between name_components. Default = ("_", "-")
    Returns:
        Path: Filename within obspack

    Examples:
        >>> define_filename(["species", "site", "inlet"], metadata: {"species":"ch4", "site":"tac", "inlet":"42m"})
        Path("ch4_tac_42m_v1.nc")

        >>> define_filename([["species", "sector"], "site", "inlet"], metadata: {"species":"ch4", "sector": "anthro", "site":"tac", "inlet":"42m"})
        Path("ch4-anthro_tac_42m_v1.nc")

        >>> define_filename(["species", "site", "inlet"], metadata: {"species":"ch4", "site":"tac", "inlet":"42m", }, obs_type="surface-insitu")
        Path("ch4_tac_42m_surface-insitu_v1.nc")
    """

    file_extension = ".nc"

    if isinstance(separators, str):
        separators = (separators,)

    filename_stub = _construct_name(name_components, metadata, separators)

    suffixes = list(name_suffixes.values())

    all_components = [filename_stub] + suffixes
    filename_stub = separators[0].join(all_components)

    filename = Path(f"{filename_stub}{file_extension}")

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


def define_subfolder(subfolder: MultiSubFolder | None, obs_type: str | None = None) -> Path:
    """
    Define the default subfolder within the obspack for a file based on obs_type.

    Args:
        subfolder: By default the obs_type will be used to create a subfolder structure. Specifying subfolder directly, supercedes
            this. This can be specified as:
                - no subfolder(s) - pass empty string
                - one subfolder for all files
                - dictionary of subfolders per obs_type.
                  - if obs_type is not within dictionary - obs_type will be used as subfolder name
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
    Returns:
        Path: subfolder
    """
    if isinstance(subfolder, dict):
        if obs_type is not None:
            if obs_type in subfolder:
                subfolder = subfolder[obs_type]
            else:
                subfolder = None
        else:
            msg = f"Unable to use subfolder input: {subfolder} if obs_type is not specified."
            logger.exception(msg)
            raise ValueError(msg)

    if subfolder is None:
        if obs_type is not None:
            subfolder = obs_type

    subfolder = _create_path(subfolder)

    return subfolder


def define_surface_filename(
    metadata: dict,
    obs_type: str | None = None,
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
    )

    return filename


def define_column_filename(
    metadata: dict,
    obs_type: str = "column",
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
    )

    return filename


# CALLED BY StoredData
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


def _create_path(input: pathType | None) -> Path:

    if input is None:
        input = Path("")
    else:
        input = Path(input)

    return input


# CALLED BY StoredData
def define_full_obspack_filename(
    filename: pathType,
    obspack_name: str | None = None,
    output_folder: pathType | None = None,
    subfolder: MultiSubFolder | None = None,
    obs_type: str | None = None,
) -> Path:
    """
    Define full path for the output filename. This is based on the structure:

        {output_folder} / {obspack_name} / {subfolder} / {filename}

    Args:
        filename: Descriptive filename for the output file
        obspack_name: Full name for the obspack
        output_folder: Top level directory.
        subfolder: By default the obs_type will be used to create a subfolder structure. Specifying subfolder directly, supercedes
            this. This can be specified as:
                - no subfolder(s) - pass empty string
                - one subfolder for all files
                - dictionary of subfolders per obs_type.
                  - if obs_type is not within dictionary - obs_type will be used as subfolder name
    Returns:
        Path: full output file path
    """

    obspack_name = obspack_name if obspack_name is not None else ""
    output_folder = _create_path(output_folder)
    subfolder = define_subfolder(subfolder, obs_type)

    return output_folder / obspack_name / subfolder / filename


# CALLED BY StoredData
def define_stored_data_filename(
    metadata: dict,
    obs_type: str,
    include_obs_type: bool = True,
    include_version: bool = True,
    data_version: str | None = None,
    name_components: MultiNameComponents | None = None,
    name_suffixes: dict | None = None,
) -> Path:
    """
    Create file name for obspack files with expected naming convention. This will
    depend on the obs_type.

    Args:
        metadata: Dictionary containing metadata values
        obs_type: Name of the observation type to be included in the obspack.
            See define_obs_types() for details of obs_type values.
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

    if obs_type in obs_types:

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
                include_obs_type=include_obs_type,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
                name_suffixes=name_suffixes,
            )
    else:
        raise ValueError(f"Did not recognise obs_type {obs_type}. Should be one of: {obs_types}")

    return filename


# CALLED by ObsPack
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
        name_components: Current metadata keys used to define a filename.
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


# CALLED by ObsPack
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


# CALLED by ObsPack
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
