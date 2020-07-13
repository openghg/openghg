from HUGS.Modules import BaseModule

__all__ = ["GC"]


class GC(BaseModule):
    _root = "GC"
    _uuid = "8cba4797-510c-gcgc-8af1-e02a5ee57489"

    def __init__(self):
        from json import load
        from Acquire.ObjectStore import get_datetime_now
        from HUGS.Util import get_datapath

        self._creation_datetime = get_datetime_now()
        self._stored = False
        self._datasources = []
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Holds parameters used for writing attributes to Datasets
        self._gc_params = {}
        # Site codes for inlet readings
        self._site_codes = {}
        self._sampling_period = 0

        params_file = get_datapath(filename="process_gcwerks_parameters.json")

        with open(params_file, "r") as f:
            data = load(f)
            self._gc_params = data["GC"]

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes

        return data

    def save(self, bucket=None):
        """ Save this GC object in the object store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                None
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        gc_key = "%s/uuid/%s" % (GC._root, GC._uuid)

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=gc_key, data=self.to_data())

    @staticmethod
    def read_file(
        data_filepath,
        precision_filepath,
        source_name,
        site,
        instrument_name=None,
        source_id=None,
        overwrite=False,
    ):
        """ Reads a GC data file by creating a GC object and associated datasources

            TODO - should this default to GCMD when no instrument is passed?

            Args:
                data_filepath (str, pathlib.Path): Path of data file
                precision_filepath (str, pathlib.Path): Path of precision file
            Returns:
                dict: Dictionary of source_name : UUIDs
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources
        from pathlib import Path
        from warnings import warn

        gc = GC.load()

        data_filepath = Path(data_filepath)

        # We need to have the 3 character site code here
        if len(site) != 3:
            site = gc.get_site_code(site)

        # Try and find the instrument name in the filename
        if instrument_name is None:
            if(len(data_filepath.stem.split("-")) > 1):
                instrument_name = data_filepath.stem.split("-")[1]
            else:
                instrument_name = "NA"

            if(not gc.is_valid_instrument(instrument_name)):
                warn(f"Invalid instrument, defaulting to GCMD. Instruments \
                        that can be read from filename are {gc._gc_params['suffix_to_instrument'].keys()}")
                instrument_name = "GCMD"

        gas_data = gc.read_data(
            data_filepath=data_filepath,
            precision_filepath=precision_filepath,
            site=site,
            instrument=instrument_name,
        )

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = gc.assign_attributes(data=gas_data, site=site)

        lookup_results = lookup_gas_datasources(
            lookup_dict=gc._datasource_names,
            gas_data=gas_data,
            source_name=source_name,
            source_id=source_id,
        )

        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = assign_data(
            gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite
        )
        # Add the Datasources to the list of datasources associated with this object
        gc.add_datasources(datasource_uuids)
        # Save object to object store
        gc.save()

        # Return the UUIDs of the datasources in which the data was stored
        return datasource_uuids

    def read_data(self, data_filepath, precision_filepath, site, instrument):
        """ Read data from the data and precision files

            Args:
                data_filepath (pathlib.Path): Path of data file
                precision_filepath (pathlib.Path): Path of precision file
                site (str): Name of site
                instrument (str): Identifying data for instrument
            Returns:
                list: List of tuples (str, dict, str, Pandas.Dataframe)

                Tuple contains species name, species metadata, datasource_uuid and dataframe
        """
        from datetime import datetime
        from pandas import read_csv
        from pandas import Timedelta as pd_Timedelta
        from HUGS.Processing import read_metadata

        # Read header
        header = read_csv(data_filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

        # Create a function to parse the datetime in the data file
        def parser(date):
            return datetime.strptime(date, "%Y %m %d %H %M")

        # Read the data in and automatically create a datetime column from the 5 columns
        # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
        data = read_csv(
            data_filepath,
            skiprows=4,
            sep=r"\s+",
            index_col=["yyyy_mm_dd_hh_mi"],
            parse_dates=[[1, 2, 3, 4, 5]],
            date_parser=parser,
        )
        data.index.name = "Datetime"

        metadata = read_metadata(filepath=data_filepath, data=data, data_type="GC")

        units = {}
        scale = {}

        species = []
        columns_renamed = {}
        for column in data.columns:
            if "Flag" in column:
                # Location of this column in a range (0, n_columns-1)
                col_loc = data.columns.get_loc(column)
                # Get name of column before this one for the gas name
                gas_name = data.columns[col_loc - 1]
                # Add it to the dictionary for renaming later
                columns_renamed[column] = gas_name + "_flag"
                # Create 2 new columns based on the flag columns
                data[gas_name + " status_flag"] = (data[column].str[0] != "-").astype(
                    int
                )
                data[gas_name + " integration_flag"] = (
                    data[column].str[1] != "-"
                ).astype(int)

                col_shift = 4
                units[gas_name] = header.iloc[1, col_loc + col_shift]
                scale[gas_name] = header.iloc[0, col_loc + col_shift]

                # Ensure the units and scale have been read in correctly
                # Have this in case the column shift between the header and data changes
                if units[gas_name] == "--" or scale[gas_name] == "--":
                    raise ValueError(
                        "Error reading units and scale, ensure columns are correct \
                        between header and dataframe"
                    )

                species.append(gas_name)

        # Rename columns to include the gas this flag represents
        data = data.rename(columns=columns_renamed, inplace=False)

        # Read and parse precisions file
        precision, precision_species = self.read_precision(precision_filepath)

        for sp in species:
            precision_index = precision_species.index(sp) * 2 + 1
            data[sp + " repeatability"] = (
                precision[precision_index]
                .astype(float)
                .reindex_like(data, method="pad")
            )

        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        self._sampling_period = self.get_precision(instrument)

        data["new_time"] = data.index - pd_Timedelta(
            seconds=self._sampling_period / 2.0
        )

        data = data.set_index("new_time", inplace=False, drop=True)
        data.index.name = "time"

        gas_data = self.split_species(
            data=data,
            site=site,
            species=species,
            instrument=instrument,
            metadata=metadata,
            units=units,
            scale=scale,
        )

        return gas_data

    def read_precision(self, filepath):
        """ Read GC precision file

            Args:
                filepath (pathlib.Path): Path of precision file
            Returns:
                tuple (Pandas.DataFrame, list): Precision DataFrame and list of species in
                precision data
        """
        from pandas import read_csv
        from datetime import datetime

        # Function for parsing datetime
        def parser(date):
            return datetime.strptime(date, "%y%m%d")

        # Read precision species
        precision_header = read_csv(
            filepath, skiprows=3, nrows=1, header=None, sep=r"\s+"
        )

        precision_species = precision_header.values[0][1:].tolist()

        precision = read_csv(
            filepath,
            skiprows=5,
            header=None,
            sep=r"\s+",
            index_col=0,
            parse_dates=[0],
            date_parser=parser,
        )

        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species

    def split_species(self, data, site, instrument, species, metadata, units, scale):
        """ Splits the species into separate dataframe into sections to be stored within individual Datasources

            Args:
                data (Pandas.DataFrame): DataFrame of raw data
                site (str): Name of site from which this data originates
                instrument (str): Name of instrument
                species (list): List of species contained in data
                metadata (dict): Dictionary of metadata
                units (dict): Dictionary of units for each species
                scale (dict): Dictionary of scales for each species
            Returns:
                dict: Dataframe of gas data and metadata
        """
        # Create a list tuples of the split dataframe and the daterange it covers
        # As some (years, months, weeks) may be empty we don't want those dataframes

        # Read inlets from the parameters dictionary
        # expected_inlets = self.get_inlets(site_code=site)
        # Get the inlets in the dataframe
        try:
            data_inlets = data["Inlet"].unique()
        except KeyError:
            raise KeyError(
                "Unable to read inlets from data, please ensure this data is of the GC \
                                    type expected by this processing module"
            )
        # TODO - ask Matt/Rachel about inlets
        matching_inlets = data_inlets

        # # For now just add air to the expected inlets
        # expected_inlets.append("air")
        # # Check that each inlet in data_inlet matches one that's given by parameters file
        # for data_inlet in data_inlets:
        #     match = [fnmatch(data_inlet, inlet) for inlet in expected_inlets]
        #     if True in match:
        #         # Filter the expected inlets by the ones we've found in data
        #         # If none of them match processing below will not proceed
        #         matching_inlets = list(compress(data_inlets, match))
        #     else:
        #         raise ValueError("Inlet mismatch - please ensure correct site is selected. \
        #                           Mismatch between inlet in data and inlet in parameters file.")

        combined_data = {}

        for spec in species:
            # Skip this species if the data is all NaNs
            if data[spec].isnull().all():
                continue

            # Create a copy of metadata for local modification
            spec_metadata = metadata.copy()

            spec_metadata["species"] = spec
            spec_metadata["units"] = units[spec]
            spec_metadata["scale"] = scale[spec]

            for inlet in matching_inlets:
                spec_metadata["inlet"] = inlet
                # If we've only got a single inlet
                if inlet == "any" or inlet == "air":
                    spec_data = data[
                        [
                            spec,
                            spec + " repeatability",
                            spec + " status_flag",
                            spec + " integration_flag",
                            "Inlet",
                        ]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")
                elif "date" in inlet:
                    dates = inlet.split("_")[1:]
                    slice_dict = {"time": slice(dates[0], dates[1])}
                    data_sliced = data.loc(slice_dict)
                    spec_data = data_sliced[
                        [
                            spec,
                            spec + " repeatability",
                            spec + " status_flag",
                            spec + " integration_flag",
                            "Inlet",
                        ]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")
                else:
                    # Take only data for this inlet from the dataframe
                    inlet_data = data.loc[data["Inlet"] == inlet]
                    spec_data = inlet_data[
                        [
                            spec,
                            spec + " repeatability",
                            spec + " status_flag",
                            spec + " integration_flag",
                            "Inlet",
                        ]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")

                attributes = self.site_attributes(
                    site=site, inlet=inlet, instrument=instrument
                )

                # We want an xarray Dataset
                spec_data = spec_data.to_xarray()

                combined_data[spec] = {}
                combined_data[spec]["metadata"] = spec_metadata
                combined_data[spec]["data"] = spec_data
                combined_data[spec]["attributes"] = attributes

        return combined_data

    def assign_attributes(self, data, site, network=None):
        """ Assign attributes to the data we've processed

            Args:
                combined_data (dict): Dictionary containing data, metadata and attributes
            Returns:
                dict: Dictionary of combined data with correct attributes assigned to Datasets
        """
        from HUGS.Processing import get_attributes

        for species in data:
            site_attributes = data[species]["attributes"]
            units = data[species]["metadata"]["units"]
            scale = data[species]["metadata"]["scale"]

            data[species]["data"] = get_attributes(
                ds=data[species]["data"],
                species=species,
                site=site,
                network=network,
                units=units,
                scale=scale,
                global_attributes=site_attributes,
                sampling_period=self._sampling_period,
            )

        return data

    def is_valid_instrument(self, instrument):
        """ Check if the instrument string passed is valid

            Returns:
                bool: True if valid
        """
        valid_instruments = self._gc_params["suffix_to_instrument"].keys()
        if(instrument.lower() in valid_instruments):
            return True
        else:
            return False

    def get_precision(self, instrument):
        """ Get the precision of the instrument in seconds

            Args:
                instrument (str): Instrument name
            Returns:
                int: Precision of instrument in seconds

        """
        if not self._gc_params:
            self.load_params()

        try:
            sampling_period = self._gc_params["sampling_period"][instrument]
        except KeyError:
            raise KeyError(f"Invalid instrument: {instrument}\nPlease select one of {self._gc_params['sampling_period'].keys()}\n")

        return sampling_period

    def get_inlets(self, site_code):
        """ Get the inlets used at this site

            Args:
                site (str): Site of datasources
            Returns:
                list: List of inlets
        """
        if not self._gc_params:
            self.load_params()

        return self._gc_params[site_code]["inlets"]

    def load_params(self):
        """ Load the parameters from file

            Returns:
                None
        """
        from json import load
        from HUGS.Util import get_datapath

        params_file = get_datapath(filename="process_gcwerks_parameters.json")

        with open(params_file, "r") as f:
            data = load(f)
            self._gc_params = data["GC"]

    def get_site_code(self, site):
        """ Get the site code

            Args:
                site (str): Name of site
            Returns:
                str: Site code
        """
        from json import load
        from HUGS.Util import get_datapath

        if not self._site_codes:
            site_codes_json = get_datapath(filename="site_codes.json")
            with open(site_codes_json, "r") as f:
                d = load(f)
                self._site_codes = d["name_code"]

        try:
            site_code = self._site_codes[site.lower()]
        except KeyError:
            raise KeyError("Site not recognized")

        return site_code

    def site_attributes(self, site, inlet, instrument):
        """ Gets the site specific attributes for writing to Datsets

            Args:
                site (str): Site name
                inlet (str): Inlet (example: 108m)
            Returns:
                dict: Dictionary of attributes
        """
        if not self._gc_params:
            self.load_params()

        attributes = self._gc_params[site.upper()]["global_attributes"]

        attributes["inlet_height_magl"] = inlet
        try:
            attributes["comment"] = self._gc_params["comment"][instrument]
        except KeyError:
            valid_instruments = list(self._gc_params["comment"].keys())
            raise KeyError(
                f"Invalid instrument passed, valid instruments : {valid_instruments}"
            )

        return attributes
