from HUGS.Modules import BaseModule

__all__ = ["NOAA"]


class NOAA(BaseModule):
    """ Interface for processing NOAA data

    """

    _root = "NOAA"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _uuid = "834316aa-a0a1-4b33-810a-7040615426b8"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now

        self._creation_datetime = get_datetime_now()
        self._stored = False
        # self._datasources = []
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Holds parameters used for writing attributes to Datasets
        self._noaa_params = {}
        # Sampling period of NOAA data in seconds
        self._sampling_period = "NA"

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes

        return data

    def save(self, bucket=None):
        """ Save the object to the object store

            Args:
                bucket (dict, default=None): Bucket for data
            Returns:
                None
        """
        from HUGS.ObjectStore import get_bucket, set_object_from_json

        if bucket is None:
            bucket = get_bucket()

        key = f"{NOAA._root}/uuid/{NOAA._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def read_folder(folder_path, extension=".dat", recursive=True):
        """ Read all data matching filter in folder

            Args:
                folder_path (str): Path of folder
                extension (str, default=".dat"): File extension for data files in folder
                recursive (bool, default=True)
            Returns:
                None
        """
        from glob import glob
        from os import path

        datasource_uuids = {}

        # This finds data files in sub-folders
        folder_path = path.join(folder_path, f"./*.{extension}")
        # This finds files in the current folder, get recursive
        # folder_path = _path.join(folder_path, "*.dat")
        filepaths = glob(folder_path, recursive=recursive)

        if not filepaths:
            raise FileNotFoundError("No data files found")

        for fp in filepaths:
            datasource_uuids[fp] = NOAA.read_file(data_filepath=fp)

        return datasource_uuids

    @staticmethod
    def read_file(
        data_filepath,
        species=None,
        source_name=None,
        site=None,
        source_id=None,
        overwrite=False,
    ):
        """ Reads NOAA data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources, get_attributes
        from HUGS.Util import hash_file
        from pathlib import Path

        noaa = NOAA.load()

        # Check if the file has been uploaded already
        file_hash = hash_file(filepath=data_filepath)
        if file_hash in noaa._file_hashes and not overwrite:
            raise ValueError(
                f"This file has been uploaded previously with the filename : {noaa._file_hashes[file_hash]}"
            )

        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        if species is None:
            species = filename.split("_")[0].upper()

        if source_name is None:
            source_name = data_filepath.stem
            source_name = source_name.split("-")[0]

        gas_data = noaa.read_data(data_filepath=data_filepath, species=species)

        if site is None:
            site = gas_data[species]["metadata"]["site"]

        for species in gas_data:
            units = noaa._noaa_params["unit_species"][species]
            # scale = noaa._noaa_params["scale"][species]
            # Unit scales used for each species
            species_scales = noaa._noaa_params["scale"][species.upper()]

            gas_data[species]["data"] = get_attributes(
                ds=gas_data[species]["data"],
                species=species,
                site="TMB",
                units=units,
                scale=species_scales,
            )

        # Check if we've got data from these sources before
        lookup_results = lookup_gas_datasources(
            lookup_dict=noaa._datasource_names,
            gas_data=gas_data,
            source_name=source_name,
            source_id=source_id,
        )

        # Assign the data to the correct datasources
        datasource_uuids = assign_data(
            gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite
        )

        # Add the Datasources to the list of datasources associated with this object
        noaa.add_datasources(datasource_uuids)

        # Store the hash as the key for easy searching, store the filename as well for
        # ease of checking by user
        noaa._file_hashes[file_hash] = filename

        noaa.save()

        return datasource_uuids

    def read_data(self, data_filepath, species, measurement_type="flask"):
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
                species (str): Species string such as CH4, CO
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from HUGS.Util import read_header
        from pandas import read_csv, Timestamp
        import numpy as np

        header = read_header(filepath=data_filepath)

        column_names = header[-1][14:].split()

        def date_parser(year, month, day, hour, minute, second):
            return Timestamp(year, month, day, hour, minute, second)

        date_parsing = {
            "time": [
                "sample_year",
                "sample_month",
                "sample_day",
                "sample_hour",
                "sample_minute",
                "sample_seconds",
            ]
        }

        data_types = {
            "sample_year": np.int,
            "sample_month": np.int,
            "sample_day": np.int,
            "sample_hour": np.int,
            "sample_minute": np.int,
            "sample_seconds": np.int,
        }

        # Number of header lines to skip
        n_skip = len(header)

        data = read_csv(
            data_filepath,
            skiprows=n_skip,
            names=column_names,
            sep=r"\s+",
            dtype=data_types,
            parse_dates=date_parsing,
            date_parser=date_parser,
            index_col="time",
            skipinitialspace=True,
        )

        data = data.loc[~data.index.duplicated(keep="first")]

        # Check if the index is sorted
        if not data.index.is_monotonic_increasing:
            data.sort_index()

        # Read the site code from the Dataframe
        site = str(data["sample_site_code"][0])
        site = site.upper()

        species = species.upper()

        flag = []
        selection_flag = []
        for flag_str in data.analysis_flag:
            flag.append(flag_str[0] == ".")
            selection_flag.append(int(flag_str[1] != "."))

        combined_data = {}

        data[species + "_status_flag"] = flag
        data[species + "_selection_flag"] = selection_flag

        data = data[data[species + "_status_flag"]]

        data = data[
            [
                "sample_latitude",
                "sample_longitude",
                "sample_altitude",
                "analysis_value",
                "analysis_uncertainty",
                species + "_selection_flag",
            ]
        ]

        rename_dict = {
            "analysis_value": species,
            "analysis_uncertainty": species + "_repeatability",
            "sample_longitude": "longitude",
            "sample_latitude": "latitude",
            "sample_altitude": "altitude",
        }

        data = data.rename(columns=rename_dict, inplace=False)

        data = data.to_xarray()

        site_attributes = self.site_attributes()

        metadata = {}
        metadata["species"] = species
        metadata["site"] = site
        metadata["measurement_type"] = measurement_type

        combined_data[species] = {
            "metadata": metadata,
            "data": data,
            "attributes": site_attributes,
        }

        return combined_data

    def site_attributes(self):
        """ Gets the site specific attributes for writing to Datsets

            Returns:
                dict: Dictionary of site attributes
        """
        if not self._noaa_params:
            from json import load
            from HUGS.Util import get_datapath

            filepath = get_datapath(filename="attributes.json")

            with open(filepath, "r") as f:
                data = load(f)
                self._noaa_params = data["NOAA"]

        attributes = self._noaa_params["global_attributes"]
        attributes["inlet_height_magl"] = "NA"
        attributes["instrument"] = self._noaa_params["instrument"]

        return attributes
