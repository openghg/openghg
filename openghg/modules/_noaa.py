from openghg.modules import BaseModule
from pathlib import Path
from typing import Dict, Optional, Union

__all__ = ["NOAA"]


class NOAA(BaseModule):
    """ Class for processing NOAA data

    """

    def __init__(self):
        from openghg.util import load_hugs_json

        # Holds parameters used for writing attributes to Datasets
        data = load_hugs_json("attributes.json")
        self._noaa_params = data["NOAA"]
        self._site_data = load_hugs_json("acrg_site_info.json")

    def read_file(
        self,
        data_filepath: Union[str, Path],
        species: Optional[str] = None,
        site: Optional[str] = None,
        network: Optional[str] = None,
    ) -> Dict:
        """ Reads NOAA data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                data_filepath: Path of file to load
                species: Species name
                site: Site name
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from openghg.processing import assign_attributes
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

        gas_data = assign_attributes(data=gas_data, site=site, network="NOAA")

        return gas_data

    def read_data(self, data_filepath: Path, species: str, measurement_type: Optional[str] = "flask") -> Dict:
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath: Path of datafile
                species: Species string such as CH4, CO
                measurement_type: Type of measurements e.g. flask
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from openghg.util import read_header
        from pandas import read_csv, Timestamp
        import numpy as np

        header = read_header(filepath=data_filepath)

        column_names = header[-1][14:].split()

        def date_parser(year, month, day, hour, minute, second):
            return Timestamp(year, month, day, hour, minute, second)

        date_parsing = {"time": ["sample_year", "sample_month", "sample_day", "sample_hour", "sample_minute", "sample_seconds"]}

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

        # Drop duplicates
        data = data.loc[~data.index.duplicated(keep="first")]

        # Check if the index is sorted
        if not data.index.is_monotonic_increasing:
            data = data.sort_index()

        # Read the site code from the Dataframe
        site = str(data["sample_site_code"][0]).upper()

        # If this isn't a site we recognize try and read it from the filename
        if site not in self._site_data:
            site = str(data_filepath.name).split("_")[1].upper()

            if site not in self._site_data:
                raise ValueError(f"The site {site} is not recognized.")

        if species is not None:
            # If we're passed a species ensure that it is in fact the correct species
            data_species = str(data["parameter_formula"].values[0]).lower()

            passed_species = species.lower()
            if data_species != passed_species:
                raise ValueError(f"Mismatch between passed species ({passed_species}) and species read from data ({data_species})")

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
