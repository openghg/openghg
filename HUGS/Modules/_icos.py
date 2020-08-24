from HUGS.Modules import BaseModule

__all__ = ["ICOS"]


class ICOS(BaseModule):
    """ Interface for processing ICOS data

    """
    def __init__(self):
        # Sampling period of ICOS data in seconds
        self._sampling_period = "NA"

    def read_file(self, data_filepath, source_name=None, site=None, network=None, overwrite=False):
        """ Reads ICOS data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from pathlib import Path

        data_filepath = Path(data_filepath)

        if source_name is None:
            source_name = data_filepath.stem

        if site is None:
            site = source_name.split(".")[0]

        species = source_name.split(".")[1]

        # This should return xarray Datasets
        gas_data = self.read_data(data_filepath=data_filepath, species=species, site=site)
        # Assign attributes to the xarray Datasets here data here makes it a lot easier to test
        gas_data = self.assign_attributes(data=gas_data, site=site, network=network)

        return gas_data

    def read_data(self, data_filepath, species, site=None):
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            # TODO - update this to process multiple species here?

            Args:
                data_filepath (pathlib.Path): Path of datafile
                species (str): Species to process
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import read_csv, Timestamp
        import numpy as np
        from HUGS.Util import read_header

        # metadata = read_metadata(filepath=data_filepath, data=data, data_type="ICOS")
        header = read_header(filepath=data_filepath)
        n_skip = len(header) - 1
        species = "co2"

        def date_parser(year, month, day, hour, minute):
            return Timestamp(year, month, day, hour, minute)

        datetime_columns = {"time": ["Year", "Month", "Day", "Hour", "Minute"]}

        use_cols = [
            "Year",
            "Month",
            "Day",
            "Hour",
            "Minute",
            str(species.lower()),
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
            index_col="time",
            sep=" ",
            usecols=use_cols,
            dtype=dtypes,
            na_values="-999.99",
            date_parser=date_parser,
        )

        data = data[data[species.lower()] >= 0.0]

        # Drop duplicate indices
        data = data.loc[~data.index.duplicated(keep="first")]

        # Check if the index is sorted
        if not data.index.is_monotonic_increasing:
            data.sort_index()

        rename_dict = {
            "Stdev": species + " variability",
            "NbPoints": species + " number_of_observations",
        }

        data = data.rename(columns=rename_dict)

        # Conver to xarray Dataset
        data = data.to_xarray()

        combined_data = {}

        site_attributes = {}

        metadata = {}
        metadata["species"] = species

        combined_data[species] = {
            "metadata": metadata,
            "data": data,
            "attributes": site_attributes,
        }

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

            # TODO - save Dataset attributes to metadata for storage within Datasource
            data[species]["data"] = get_attributes(
                ds=data[species]["data"],
                species=species,
                site=site,
                network=network,
                global_attributes=site_attributes,
                sampling_period=self._sampling_period,
            )

        return data
