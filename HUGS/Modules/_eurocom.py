from HUGS.Modules import BaseModule

__all__ = ["EUROCOM"]


class EUROCOM(BaseModule):
    """ Interface for processing EUROCOM data

        This is only a temporary module to processing the ICOS EUROCOM study data

        ICOS data processing will be done in the ICOS module

    """

    _root = "EUROCOM"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _uuid = "de3e5930-f995-48d7-b3b8-153112b626ee"

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
        self._eurocom_params = {}
        # Sampling period of EUROCOM data in seconds
        self._sampling_period = 60

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
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = f"{EUROCOM._root}/uuid/{EUROCOM._uuid}"

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

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
            datasource_uuids[fp] = EUROCOM.read_file(data_filepath=fp)

        return datasource_uuids

    @staticmethod
    def read_file(
        data_filepath, source_name, site=None, source_id=None, overwrite=False
    ):
        """ Reads EUROCOM data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources
        from HUGS.Util import hash_file
        from pathlib import Path

        eurocom = EUROCOM.load()

        # Check if the file has been uploaded already
        file_hash = hash_file(filepath=data_filepath)
        if file_hash in eurocom._file_hashes and not overwrite:
            raise ValueError(
                f"This file has been uploaded previously with the filename: {eurocom._file_hashes[file_hash]}"
            )

        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        if not source_name:
            source_name = data_filepath.stem

        if not site:
            site = source_name.split("_")[0]

        # This should return xarray Datasets
        gas_data = eurocom.read_data(data_filepath=data_filepath, site=site)

        # Check if we've got data from these sources before
        lookup_results = lookup_gas_datasources(
            lookup_dict=eurocom._datasource_names,
            gas_data=gas_data,
            source_name=source_name,
            source_id=source_id,
        )

        # Assign the data to the correct datasources
        datasource_uuids = assign_data(
            gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite
        )

        # Add the Datasources to the list of datasources associated with this object
        eurocom.add_datasources(datasource_uuids)

        # Store the hash as the key for easy searching, store the filename as well for
        # ease of checking by user
        eurocom._file_hashes[file_hash] = filename

        eurocom.save()

        return datasource_uuids

    def read_data(self, data_filepath, site, height=None):
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import read_csv, Timestamp
        import numpy as np
        from HUGS.Processing import get_attributes
        from HUGS.Util import read_header
        from pathlib import Path

        data_filepath = Path(data_filepath)

        filename = data_filepath.name
        inlet_height = filename.split("_")[1]
        if "m" not in inlet_height:
            inlet_height = None

        # This dictionary is used to store the gas data and its associated metadata
        combined_data = {}

        # Read the header as lines starting with #
        header = read_header(data_filepath, comment_char="#")
        n_skip = len(header) - 1
        species = "co2"

        def date_parser(year, month, day, hour, minute):
            return Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)

        datetime_columns = {"time": ["Year", "Month", "Day", "Hour", "Minute"]}
        use_cols = [
            "Day",
            "Month",
            "Year",
            "Hour",
            "Minute",
            str(species.lower()),
            "SamplingHeight",
            "Stdev",
            "NbPoints",
        ]

        dtypes = {
            "Day": np.int,
            "Month": np.int,
            "Year": np.int,
            "Hour": np.int,
            "Minute": np.int,
            species.lower(): np.float,
            "Stdev": np.float,
            "SamplingHeight": np.float,
            "NbPoints": np.int,
        }

        data = read_csv(
            data_filepath,
            skiprows=n_skip,
            parse_dates=datetime_columns,
            date_parser=date_parser,
            index_col="time",
            sep=";",
            usecols=use_cols,
            dtype=dtypes,
            na_values="-999.99",
        )

        data = data[data[species.lower()] >= 0.0]
        data = data.dropna(axis="rows", how="any")
        # Drop duplicate indices
        data = data.loc[~data.index.duplicated(keep="first")]
        # Convert to xarray Dataset
        data = data.to_xarray()

        site_attributes = self.site_attributes(site=site, inlet=inlet_height)

        try:
            calibration_scale = self._eurocom_params["calibration"][site]
        except KeyError:
            calibration_scale = {}

        gas_data = get_attributes(
            ds=data,
            species=species,
            site=site,
            global_attributes=site_attributes,
            units="ppm",
        )

        # Create a copy of the metadata dict
        metadata = {}
        metadata["site"] = site
        metadata["species"] = species
        metadata["inlet_height"] = site_attributes["inlet_height_m"]
        metadata["calibration_scale"] = calibration_scale

        combined_data[species] = {
            "metadata": metadata,
            "data": gas_data,
            "attributes": site_attributes,
        }

        return combined_data

    def site_attributes(self, site, inlet):
        """ Gets the site specific attributes for writing to Datsets

            Args:
                site (str): Site name
            Returns:
                dict: Dictionary of attributes
        """
        site = site.upper()

        if not self._eurocom_params:
            from json import load
            from HUGS.Util import get_datapath

            filepath = get_datapath(filename="attributes.json")

            with open(filepath, "r") as f:
                data = load(f)
                self._eurocom_params = data["EUROCOM"]

        attributes = self._eurocom_params["global_attributes"]

        if not inlet:
            if site in self._eurocom_params["intake_height"]:
                inlet = self._eurocom_params["intake_height"][site]
            else:
                inlet = "NA"

        attributes["inlet_height_m"] = str(inlet)

        return attributes
