from HUGS.Modules import BaseModule

__all__ = ["NOAA"]


class NOAA(BaseModule):
    """ Interface for processing NOAA data

    """
    def __init__(self):
        from HUGS.Util import load_hugs_json

        # Holds parameters used for writing attributes to Datasets
        data = load_hugs_json("attributes.json")
        self._noaa_params = data["NOAA"]

    def read_file(
        self,
        data_filepath,
        species=None,
        site=None,
        overwrite=False,
    ):
        """ Reads NOAA data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from HUGS.Processing import assign_attributes
        from pathlib import Path

        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        if species is None:
            species = filename.split("_")[0].lower()

        source_name = data_filepath.stem
        source_name = source_name.split("-")[0]

        gas_data = self.read_data(data_filepath=data_filepath, species=species)

        if site is None:
            site = gas_data[species.lower()]["metadata"]["site"]

        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data

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

        site_attributes = self._noaa_params["global_attributes"]
        site_attributes["inlet_height_magl"] = "NA"
        site_attributes["instrument"] = self._noaa_params["instrument"][species.upper()]

        metadata = {}
        metadata["species"] = species.lower()
        metadata["site"] = site
        metadata["measurement_type"] = measurement_type
        metadata["network"] = "NOAA"

        combined_data[species.lower()] = {
            "metadata": metadata,
            "data": data,
            "attributes": site_attributes,
        }

        return combined_data
