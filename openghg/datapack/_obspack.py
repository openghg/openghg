"""
This module is allow the creation of an output data obspack based on files stored within the object store.

An obspack is a collection of consistent observation data which can be used by the community.

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
 - define_full_obspack_filename() - defines the full output filename for each file based on naming convention
 - retrieve_data() - retrieve data from an object store search terms (currently from a config file)
 - create_obspack() - this is the summary function for creating an obspack
"""

import numpy as np
import pandas as pd
import shutil
import pathlib
from pathlib import Path

import importlib.resources
from typing import Sequence, cast
import logging

from openghg.dataobjects import ObsData, ObsColumnData
from openghg.types import pathType
from openghg.util import check_unique, find_repeats, collate_strings

from ._file_structure import (
    find_current_obspacks,
    define_obspack_name,
    find_data_version,
    define_subfolder,
    define_name_components,
    define_stored_data_filename,
    define_full_obspack_filename,
    _find_additional_metakeys,
    MultiNameComponents,
    MultiSubFolder,
)
from ._specification import define_get_functions

logger = logging.getLogger("openghg.obspack")

# TODO: Perhaps move away from the overall type definitions being in classes? (openghg.types._enum.py)
ObsOutputType = ObsData | ObsColumnData


class StoredData:
    """
    This class contains details of the data extracted from the object store with details
    of how this is related to the obspack this will be outputted to.
    """

    def __init__(
        self,
        data: ObsOutputType,
        obs_type: str = "surface-insitu",
        subfolder: MultiSubFolder | None = None,
        filename: pathType | None = None,
        data_version: str | None = None,
    ):
        """
        Creation of a StoredData object. This expects a retrieved data object from the object store.

        Args:
            data: ObsData or ObsColumnData object. This is the object returned when retrieving data
                from an object store.
            obs_type: Observation type associated with this dataset (see define_obs_types() for full list)
            subfolder: By default the obs_type will be used to create a subfolder structure. Specifying subfolder directly, supercedes
                this. This can be specified as:
                    - no subfolder(s) - pass empty string
                    - one subfolder for all files
                    - dictionary of subfolders per obs_type.
                    - if obs_type is not within dictionary - obs_type will be used as subfolder name
            filename: Output filename within obspack folder structure. See define_full_path() method for how full path to
                the file stored is constructed.
            data_version: Version of the data. If not specified this
                will attempt to extract the latest version details from the metadata.

        Note: at the moment this is specific to observation types but this could be expanded
        to include all output data types.
        """
        self.stored_data = data
        self.data = data.data
        self.metadata = data.metadata

        self.obs_type = obs_type

        self.filename = Path(filename) if filename is not None else None

        # If input filename already has folder structure, don't add a default subfolder
        if str(self.filename.parent) != ".":
            if subfolder is None:
                subfolder = ""

        self.add_subfolder(subfolder)  # Right to add default subfolder here?
        self.add_data_version(data_version)

    def define_filename(
        self,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
    ) -> Path:
        """
        Create the filename for the StoredData based on the associated metadata
        and the obs_type.

        Args:
            include_obs_type: Whether to include obs_type in the filename. Default = True.
            include_version: Whether to include the data version in the filename. Default = True.
            data_version: Version of the data. If not specified and include_version is True this
                will attempt to extract the latest version details from the metadata.
            name_components: Keys to use when extracting names from the metadata and to use
                within the filename. This can be specified per obs_type using a dictionary.
                Default will depend on obs_type - see define_name_components().
            name_suffixes: Dictionary of additional values to add to the filename as a suffix.
        Returns:
            Path: obspack filename
        """
        self.data_version: str | None = data_version if data_version is not None else self.data_version

        filename = define_stored_data_filename(
            self.metadata,
            self.obs_type,
            include_obs_type=include_obs_type,
            include_version=include_version,
            data_version=self.data_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )

        return filename

    def update_filename(
        self,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
    ) -> Path:
        """
        Define the filename and update on the StoredData object based on the associated metadata
        and the obs_type.

        Args:
            include_obs_type: Whether to include obs_type in the filename. Default = True.
            include_version: Whether to include the data version in the filename. Default = True.
            data_version: Version of the data. If not specified and include_version is True this
                will attempt to extract the latest version details from the metadata.
            name_components: Keys to use when extracting names from the metadata and to use
                within the filename. This can be specified per obs_type using a dictionary.
                Default will depend on obs_type - see define_name_components().
            name_suffixes: Dictionary of additional values to add to the filename as a suffix.
        Returns:
            Path: obspack filename
        """
        filename = self.define_filename(
            include_obs_type=include_obs_type,
            include_version=include_version,
            data_version=data_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )

        self.filename = filename
        self.filename = cast(Path, self.filename)

        return filename

    def add_subfolder(self, subfolder: MultiSubFolder | None = None) -> None:
        """
        Add the subfolder based on input and defaults.
        See define_subfolder() function.

        Returns:
            None
        """
        self.subfolder = define_subfolder(subfolder, obs_type=self.obs_type)

    def add_data_version(self, data_version: str | None = None) -> None:
        """
        Add data_version associated with StoredData.
        See find_data_version() function.

        Returns:
            None
        """
        if data_version is None:
            data_version = find_data_version(self.metadata)
        self.data_version = data_version

    def define_full_path(
        self,
        obspack_name: str | None = None,
        output_folder: pathType | None = None,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
        filename: pathType | None = None,
    ) -> Path:
        """
        Define full path for the output filename. This is based on the structure:
            {output_folder} / {obspack_name} / {subfolder} / {filename}

        Returns:
            Path: full output file path
        """

        if filename:
            self.filename = filename

        if self.filename is None:
            logger.info("Creating filename for stored data before writing to the obspack.")
            self.update_filename(
                include_obs_type=include_obs_type,
                include_version=include_version,
                data_version=data_version,
                name_components=name_components,
                name_suffixes=name_suffixes,
            )
            self.filename = cast(Path, self.filename)

        return define_full_obspack_filename(
            self.filename, obspack_name, output_folder, self.subfolder, self.obs_type
        )

    def define_site_details(self, strict: bool = False) -> dict:
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
        ds = self.data
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

        params["Observation type"] = self.obs_type

        return params

    def write(
        self,
        obspack_name: str | None = None,
        output_folder: pathType | None = None,
        include_obs_type: bool = True,
        include_version: bool = True,
        data_version: str | None = None,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
    ) -> None:
        """
        Write stored data details to file. If filename is not already defined,
        this will use the update_filename() method to create this.

        Args:
            obspack_name: Name of the obspack to write this StoredData object to
            output_folder: Top level directory containing the obspack.
            include_obs_type: Whether to include obs_type in the filename. Default = True.
                Only used if self.filename is not already defined.
            include_version: Whether to include the data version in the filename. Default = True.
                Only used if self.filename is not already defined.
            data_version: Version of the data. If not specified and include_version is True this
                will attempt to extract the latest version details from the metadata.
                Only used if self.filename is not already defined.
            name_components: Keys to use when extracting names from the metadata and to use
                within the filename. This can be specified per obs_type using a dictionary.
                Default will depend on obs_type - see define_name_components().
                Only used if self.filename is not already defined.
            name_suffixes: Dictionary of additional values to add to the filename as a suffix.
                Only used if self.filename is not already defined.
        Returns:
            None
            Writes data to disc as netcdf file.
        """

        ds = self.data
        output_filename = self.define_full_path(
            obspack_name=obspack_name,
            output_folder=output_folder,
            include_obs_type=include_obs_type,
            include_version=include_version,
            data_version=data_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )

        output_filename.parent.mkdir(parents=True, exist_ok=True)
        ds.to_netcdf(output_filename)


class ObsPack:
    """
    The ObsPack object includes details associated with how to define the an
    output obspack and what data is included within this.

    This will typically include:
        - StoredData objects (data retrieved from an object store)
        - Static helper files to be included within the obspack (release files)
        - Collated details of site information for the observation data
    """

    def __init__(self, output_folder: pathType, obspack_name: str | None = None):
        """
        Args:
            output_folder: Path to top level directory where obspack folder will be created.
            obspack_name: Full name for the obspack. This can either be specified directly
                upon initialisation of the ObsPack object or can be created using an obspack_stub
                and the self.define_obspack_name() function.
        """
        self.output_folder = Path(output_folder)
        if obspack_name is not None:
            self.obspack_name: str | None = obspack_name
            self.version: str | None = "v1"
        else:
            self.obspack_name = None
            self.version = None

        self.retrieved_data: list[StoredData] | None = None

    def find_current_obspacks(self, obspack_stub: str) -> list[Path]:
        """
        Find obspacks within the supplied output folder. Expect these will start with
        the obspack_stub string.

        Args:
            obspack_stub: Start of the obspack_name. This will be assumed to be standard for
                other obspacks of the same type.
        Returns:
            list : Path to current folders identified as current obspacks within output_folder
        """
        current_obspacks = find_current_obspacks(self.output_folder, obspack_stub)
        return current_obspacks

    def define_obspack_name(
        self,
        obspack_stub: str,
        version: str | None = None,
        major_version_only: bool = False,
        minor_version_only: bool = False,
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

        output_folder = self.output_folder

        obspack_name, version = define_obspack_name(
            obspack_stub,
            version=version,
            major_version_only=major_version_only,
            minor_version_only=minor_version_only,
            output_folder=output_folder,
            current_obspacks=current_obspacks,
        )

        self.obspack_name = obspack_name
        self.version = version

        return obspack_name, version

    def define_obspack_path(self) -> Path:
        """
        Define the full output path for the obspack folder.

        Returns:
            Path: Full obspack folder name
        """
        if self.obspack_name is None:
            raise ValueError("Please define obspack_name directly or via define_obspack_name() method.")

        return self.output_folder / self.obspack_name

    def find_and_retrieve_data(
        self,
        filename: pathType | None = None,
        search_df: pd.DataFrame | None = None,
        store: str | None = None,
        subfolders: MultiSubFolder | None = None,
    ) -> list:
        """
        Use search parameters to get data from object store. This expects either a filename for an input
        file containing search parameters (see read_input_file() for more details) or a DataFrame containing
        the search parameters.

        Args:
            filename: Filename containing search parameters.
            search_df: pandas DataFrame containing search parameters
            store: Name of specific object store to use to search for data
            subfolders: By default the obs_types will be used to create a subfolder structure. Specifying subfolders directly, supercedes
            this. This can be specified as:
             - no subfolder(s) - pass empty string
             - one subfolder for all files - pass a string
             - dictionary of subfolders per obs_type.
        Returns:
            list [StoredData]: List of extracted data from the object store based on search parameters
        """

        retrieved_data = retrieve_data(filename=filename, search_df=search_df, store=store)

        self.retrieved_data = retrieved_data

        for data in retrieved_data:
            data.output_folder = self.output_folder
            data.obspack_name = self.obspack_name

            data.add_subfolder(subfolders)

        return retrieved_data

    def contained_obs_types(self) -> list | None:
        """ """

        if self.retrieved_data is not None:
            data_obs_types = [data.obs_type for data in self.retrieved_data]
            obs_types = list(np.unique(data_obs_types))
        else:
            logger.warning("No retrieved data is present on ObsPack to find obs_types")
            obs_types = None

        return obs_types

    @staticmethod
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

    def check_retrieved_data(self) -> list[StoredData]:
        """ """
        if self.retrieved_data is None:
            msg = "ObsPack does not contain details to write to disc. Please populate ObsPack.retrieved_data"
            logger.exception(msg)
            raise ValueError(msg)

        return self.retrieved_data

    def define_stored_data_filenames(
        self,
        include_obs_type: bool = True,
        include_version: bool = True,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
        force: bool = False,
    ) -> list[Path]:
        """
        Define the filename values for multiple StoredData objects.
        If the filename is already present, this will not update by default and will
        use the stored value.

        Args:
            include_obs_type: Whether to include obs_type in the filename. Default = True.
            include_version: Whether to include the data version in the filename. Default = True.
            name_components: Keys to use when extracting names from the metadata and to use
                within the filename. This can be specified per obs_type using a dictionary.
                Default will depend on obs_type - see define_name_components().
            name_suffixes: Dictionary of additional values to add to the filename as a suffix.
            force: Force update of the filename and recreate this.
        Returns:
            list[pathlib.Path]: Sequence of filenames associated with the files
        """
        retrieved_data = self.check_retrieved_data()

        filenames = []
        for data in retrieved_data:
            filename = data.filename
            if filename is None or force:
                filename = data.update_filename(
                    include_obs_type=include_obs_type,
                    include_version=include_version,
                    name_components=name_components,
                    name_suffixes=name_suffixes,
                )
            filenames.append(filename)

        return filenames

    def check_unique_filenames(
        self,
        include_obs_type: bool = True,
        include_version: bool = True,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
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
            force: Force update of the filename and recreate this.
        Returns:
            list: Groups of StoredData objects with have overlapping filenames
        """

        filenames = self.define_stored_data_filenames(
            include_obs_type=include_obs_type,
            include_version=include_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
            force=force,
        )

        retrieved_data = self.check_retrieved_data()

        repeated_indices = find_repeats(filenames)

        data_grouped_repeats: list[list] = []
        if repeated_indices:
            data_grouped_repeats = [
                [retrieved_data[index] for index in index_set] for index_set in repeated_indices
            ]

        return data_grouped_repeats

    def add_stored_data_filenames(
        self,
        include_obs_type: bool = True,
        include_version: bool = True,
        name_components: MultiNameComponents | None = None,
        name_suffixes: dict | None = None,
        store: str | None = None,
    ) -> list[StoredData]:
        """
        Add filenames to StoredData objects in retrieved_data. This is based
        on the metadata associated with the retrieved data.
        If any filenames within the retrieved_data list are not unique, update the filename
        using more keywords within the metadata.

        Note: updates the filename attributes in place within StoredData objects.

        Args:
            include_obs_type: Whether to include obs_type in the filename. Default = True.
            include_version: Whether to include the data version in the filename. Default = True.
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
        self.define_stored_data_filenames(
            include_obs_type=include_obs_type,
            include_version=include_version,
            # data_version=data_version,
            name_components=name_components,
            force=True,
        )

        retrieved_data = self.check_retrieved_data()

        # Check for repeats and update names
        data_grouped_repeats = data_grouped_repeats = self.check_unique_filenames(
            include_obs_type=include_obs_type,
            include_version=include_version,
            name_components=name_components,
            name_suffixes=name_suffixes,
        )
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

                filenames = [data.filename for data in data_group]
                for additional_key in metakeys:
                    if check_unique(filenames):
                        break
                    else:
                        key_present = [additional_key in data.metadata for data in data_group]
                        if all(key_present):
                            name_components = name_components + [additional_key]

                            logger.info(
                                f"Checking alternative name for non-unique filename: {data_group[0].filename} with {additional_key}."
                            )
                            filenames = [
                                data.define_filename(name_components=name_components) for data in data_group
                            ]
                else:
                    raise ValueError(
                        f"Unable to find unique name for {data_group}. Please specify name_components to use."
                    )

                # Once unique names have been found add them to the data entries.
                # **Better way to do this?
                for data, filename in zip(data_group, filenames):
                    data.filename = filename

        return retrieved_data

    def define_site_index(self) -> pd.DataFrame:
        """
        Define site index for all StoredData associated with the ObsPack.
        This compiles the details a pandas DataFrame, combining rows for
        inlets of the same site.

        Returns:
            pandas.DataFrame: Site details collated as a DataFrame
        """
        site_detail_rows = []
        retrieved_data = self.check_retrieved_data()

        for data in retrieved_data:
            site_details = data.define_site_details()
            site_detail_rows.append(site_details)

        site_details_df = pd.DataFrame(site_detail_rows)
        self.site_details = site_details_df

        return site_details_df

    def write_site_index_file(self, output_filename: pathType) -> None:
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

        if not hasattr(self, "site_details"):
            self.define_site_index()

        site_details = self.site_details

        site_column = "Site code"

        if site_column not in site_details.columns:
            msg = (
                "Unable to create site details file: 'Site code' column is not present in provided DataFrame."
            )
            logger.exception(msg)
            raise ValueError(msg)

        obspack_path = self.define_obspack_path()
        full_output_filename = obspack_path / output_filename

        obspack_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing site details to: {full_output_filename}")
        output_file = open(full_output_filename, "w")

        site_details = site_details.groupby(site_column).apply(collate_strings, include_groups=False)
        site_details = site_details.dropna(axis=1, how="all")

        # Add header to file
        output_file.write("++++++++++++\n")
        output_file.write("Site details\n")
        output_file.write(f"File created: {pd.to_datetime('today').normalize()}\n")
        output_file.write("++++++++++++\n")
        output_file.write("\n")

        site_details.to_csv(output_file, index=False, sep="\t")

        output_file.close()

    def write(
        self,
        include_release_files: bool = True,
        release_files: Sequence | None = None,
        include_site_index: bool = True,
        site_index_filename: pathType | None = None,
    ) -> None:
        """
        Write constructed ObsPack to disc.
        Currently this will write:
            - retrieved_data
            - release files
            - site index file

        """
        retrieved_data = self.check_retrieved_data()

        for data in retrieved_data:
            data.write(obspack_name=self.obspack_name, output_folder=self.output_folder)

        if include_release_files:
            if release_files is not None:
                self.release_files = release_files
            elif not hasattr(self, "release_files") or self.release_files is None:
                self.release_files = ObsPack.default_release_files()

            obspack_path = self.define_obspack_path()
            obspack_path.mkdir(parents=True, exist_ok=True)

            for file in self.release_files:
                shutil.copy(file, obspack_path)

        if include_site_index:
            if site_index_filename is None:
                site_index_filename = f"site_index_details_{self.version}.txt"
            self.write_site_index_file(site_index_filename)


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
        msg = "Either filename or extracted search dataframe must be supplied to retrieve data"
        logger.exception(msg)
        raise ValueError(msg)

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
    include_release_files: bool = True,
    release_files: Sequence | None = None,
    include_site_index: bool = True,
    site_index_filename: pathType | None = None,
    subfolders: MultiSubFolder | None = None,
    include_obs_type: bool = True,
    include_data_versions: bool = True,
    name_components: MultiNameComponents | None = None,
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

    obspack = ObsPack(output_folder, obspack_name)
    obspack.find_and_retrieve_data(
        filename=search_filename, search_df=search_df, store=store, subfolders=subfolders
    )

    if obspack_name is None:
        if obspack_stub:
            obspack_name, version = obspack.define_obspack_name(
                obspack_stub,
                version=version,
                major_version_only=major_version_only,
                minor_version_only=minor_version_only,
                current_obspacks=current_obspacks,
            )
        else:
            msg = "Either obspack_name or obspack_stub must be specified when creating an obspack."
            logger.exception(msg)
            raise ValueError(msg)

    # Create default obspack filenames for data
    # If any duplicates are found and update to use more of the metadata be more specific
    obspack.add_stored_data_filenames(
        include_obs_type=include_obs_type,
        include_version=include_data_versions,
        name_components=name_components,
    )

    obspack.write(
        include_release_files=include_release_files,
        release_files=release_files,
        include_site_index=include_site_index,
        site_index_filename=site_index_filename,
    )

    obspack_path = obspack.define_obspack_path()

    return obspack_path
