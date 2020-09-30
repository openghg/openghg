__all__ = ["EUROCOM"]


class EUROCOM():
    """ Interface for processing EUROCOM data

        This is only a temporary module to processing the ICOS EUROCOM study data

        ICOS data processing will be done in the ICOS module

    """
    def __init__(self):
        from HUGS.Util import load_hugs_json

        self._eurocom_params = {}
        # Sampling period of EUROCOM data in seconds
        self._sampling_period = 60

        data = load_hugs_json(filename="attributes.json")
        self._eurocom_params = data["EUROCOM"]

    def read_file(self, data_filepath, site=None, overwrite=False):
        """ Reads EUROCOM data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from pathlib import Path
        from HUGS.Processing import assign_attributes

        data_filepath = Path(data_filepath)

        if site is None:
            site = data_filepath.stem.split("_")[0]

        # This should return xarray Datasets
        gas_data = self.read_data(data_filepath=data_filepath, site=site)

        # Assign attributes to the xarray Datasets here data here makes it a lot easier to test
        gas_data = assign_attributes(data=gas_data, site=site, sampling_period=self._sampling_period)

        return gas_data

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

        site_attributes = self.get_site_attributes(site=site, inlet=inlet_height)

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
        metadata["network"] = "EUROCOM"

        combined_data[species] = {
            "metadata": metadata,
            "data": gas_data,
            "attributes": site_attributes,
        }

        return combined_data

    def get_site_attributes(self, site, inlet):
        """ Gets the site specific attributes for writing to Datsets

            Args:
                site (str): Site name
            Returns:
                dict: Dictionary of attributes
        """
        site = site.upper()

        attributes = self._eurocom_params["global_attributes"]

        if inlet is None:
            if site in self._eurocom_params["intake_height"]:
                inlet = self._eurocom_params["intake_height"][site]
                attributes["inlet_height_m"] = str(inlet)
            else:
                raise ValueError(f"Unable to find inlet from filename or attributes file for {site}")

        return attributes
