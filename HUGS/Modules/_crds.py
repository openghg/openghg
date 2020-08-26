from HUGS.Util import load_hugs_json

__all__ = ["CRDS"]

class CRDS():
    """
        Interface for processing CRDS data
    """
    def __init__(self):
        # Holds parameters used for writing attributes to Datasets
        self._crds_params = {}
        # Sampling period of CRDS data in seconds
        self._sampling_period = 60

        data = load_hugs_json(filename="process_gcwerks_parameters.json")
        self._crds_params = data["CRDS"]

    def read_file(self, data_filepath, source_name=None, site=None):
        """ Creates a CRDS object holding data stored within Datasources

            Args:
                filepath (str): Path of file to load
                source_name (str, default=None): Name of source
                site (str, default=None): Name of site
                source_id (str, default=None): Source's unique ID
                overwrite (bool, default=False): If True overwrite any data currently stored for this date range
            Returns:
                None
        """
        from pathlib import Path
        from HUGS.Processing import assign_attributes

        if not isinstance(data_filepath, Path):
            data_filepath = Path(data_filepath)

        if not source_name:
            source_name = data_filepath.stem

        if not site:
            site = source_name.split(".")[0]

        # Process the data into separate Datasets
        gas_data = self.read_data(data_filepath=data_filepath, site=site)
        # Ensure the data is CF compliant
        gas_data = assign_attributes(data=gas_data, site=site, sampling_period=self._sampling_period)

        return gas_data

    def read_data(self, data_filepath, site):
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
            Returns:
                dict: Dictionary containing metadata, data and attributes keys
        """
        from datetime import datetime
        from pandas import RangeIndex, read_csv, NaT

        # Function to parse the datetime format found in the datafile
        def parse_date(date):
            try:
                return datetime.strptime(date, "%y%m%d %H%M%S")
            except ValueError:
                return NaT

        data = read_csv(
            data_filepath,
            header=None,
            skiprows=1,
            sep=r"\s+",
            index_col=["0_1"],
            parse_dates=[[0, 1]],
            date_parser=parse_date,
        )

        data.index.name = "time"

        # At the moment we're using the filename as the source name
        source_name = data_filepath.stem

        # -1 here as we've already removed the file extension
        # As we're not processing a list of datafiles here we'll only have one inlet
        inlet = source_name.split(".")[3]

        if "m" not in inlet.lower():
            raise ValueError("No inlet found, we expect filenames such as: bsd.picarro.1minute.108m.dat")

        # Drop any rows with NaNs
        # This is now done before creating metadata
        data = data.dropna(axis="rows", how="any")

        # Get the number of gases in dataframe and number of columns of data present for each gas
        n_gases, n_cols = self.gas_info(data=data)

        header = data.head(2)
        skip_cols = sum([header[column][0] == "-" for column in header.columns])

        header_rows = 2
        # Create metadata here
        metadata = self.read_metadata(filepath=data_filepath, data=data)

        # This dictionary is used to store the gas data and its associated metadata
        combined_data = {}

        for n in range(n_gases):
            # Slice the columns
            gas_data = data.iloc[:, skip_cols + n * n_cols : skip_cols + (n + 1) * n_cols]

            # Reset the column numbers
            gas_data.columns = RangeIndex(gas_data.columns.size)
            species = gas_data[0][0]
            species = species.lower()

            column_labels = [species, f"{species} stdev", f"{species} n_meas"]

            # Name columns
            gas_data = gas_data.set_axis(column_labels, axis="columns", inplace=False)
            # Drop the first two rows now we have the name
            gas_data = gas_data.drop(
                index=gas_data.head(header_rows).index, inplace=False
            )
            # Cast data to float64 / double
            gas_data = gas_data.astype("float64")

            # Here we can convert the Dataframe to a Dataset and then write the attributes
            gas_data = gas_data.to_xarray()

            site_attributes = self.get_site_attributes(site=site, inlet=inlet)

            # Create a copy of the metadata dict
            species_metadata = metadata.copy()
            species_metadata["species"] = species
            species_metadata["inlet"] = inlet

            # species_metadata["source_name"] = source_name

            combined_data[species] = {
                "metadata": species_metadata,
                "data": gas_data,
                "attributes": site_attributes,
            }

        return combined_data

    def read_metadata(self, filepath, data):
        """ Parse CRDS files and create a metadata dict

            Args:
                filename (str): Name of data file
                data (Pandas.DataFrame): Raw data
            Returns:
                dict: Dictionary containing metadata
        """
        # Find gas measured and port used
        type_meas = data[2][2]
        port = data[3][2]

        # Split the filename to get the site and resolution
        split_filename = str(filepath.name).split(".")

        if len(split_filename) < 4:
            raise ValueError(
                "Error reading metadata from filename. The expected format is \
                {site}.{instrument}.{time resolution}.{height}.dat"
            )

        site = split_filename[0]
        instrument = split_filename[1]
        resolution_str = split_filename[2]
        inlet = split_filename[3]

        if resolution_str == "1minute":
            resolution = "1_minute"
        elif resolution_str == "hourly":
            resolution = "1_hour"
        else:
            raise ValueError("Unable to read time resolution from filename.")

        metadata = {}
        metadata["site"] = site
        metadata["instrument"] = instrument
        metadata["time_resolution"] = resolution
        metadata["inlet"] = inlet
        metadata["port"] = port
        metadata["type"] = type_meas

        return metadata

    def get_site_attributes(self, site, inlet):
        """ Gets the site specific attributes for writing to Datsets

            Args:
                site (str): Site name
                inlet (str): Inlet (example: 108m)
            Returns:
                dict: Dictionary of attributes
        """
        from HUGS.Util import load_hugs_json

        if not self._crds_params:
            data = load_hugs_json(filename="process_gcwerks_parameters.json")
            self._crds_params = data["CRDS"]

        try:
            attributes = self._crds_params[site.upper()]["global_attributes"]
        except KeyError:
            raise ValueError(f"Unable to read attributes for site: {site}")

        attributes["inlet_height_magl"] = inlet.split("_")[0]
        attributes["comment"] = self._crds_params["comment"]

        return attributes

    def gas_info(self, data):
        """ Returns the number of columns of data for each gas
                that is present in the dataframe

                Args:
                    data (Pandas.DataFrame): Measurement data
                Returns:
                    tuple (int, int): Number of gases, number of
                    columns of data for each gas
            """
        from HUGS.Util import unanimous

        # Slice the dataframe
        head_row = data.head(1)

        gases = {}
        # Loop over the gases and find each unique value
        for column in head_row.columns:
            s = head_row[column][0]
            if s != "-":
                gases[s] = gases.get(s, 0) + 1

        # Check that we have the same number of columns for each gas
        if not unanimous(gases):
            raise ValueError(
                "Each gas does not have the same number of columns. Please ensure data"
                "is of the CRDS type expected by this module"
            )

        return len(gases), list(gases.values())[0]
