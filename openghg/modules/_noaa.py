from openghg.modules import BaseModule
from pathlib import Path
from typing import Dict, Optional, Union

__all__ = ["NOAA"]


class NOAA(BaseModule):
    """Class for processing NOAA data"""

    def __init__(self):
        from openghg.util import load_json

        # Holds parameters used for writing attributes to Datasets
        data = load_json("attributes.json")
        self._noaa_params = data["NOAA"]
        self._site_data = load_json("acrg_site_info.json")

    def read_file(
        self,
        data_filepath: Union[str, Path],
        site: str,
        inlet: str,
        measurement_type: str,
        network: Optional[str] = "NOAA",
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
    ) -> Dict:
        """Read NOAA data from raw text file or ObsPack NetCDF

        Args:
            data_filepath: Data filepath
            site: Three letter site code
            inlet: Inlet height, if no height use measurement type e.g. flask
            measurement_type: One of ("flask", "insitu", "pfp")
            network: Network, defaults to NOAA
            instrument: Instrument name
            sampling_period: Sampling period
        Returns:
            dict: Dictionary of data and metadata
        """
        from pathlib import Path

        if inlet is None:
            raise ValueError("Inlet must be given for NOAA data processing. If flask data pass flask as inlet.")

        file_extension = Path(data_filepath).suffix

        if file_extension == ".nc":
            return self.read_obspack(
                data_filepath=data_filepath,
                site=site,
                inlet=inlet,
                measurement_type=measurement_type,
                instrument=instrument,
                sampling_period=sampling_period,
            )
        else:
            return self.read_raw_file(
                data_filepath=data_filepath,
                site=site,
                inlet=inlet,
                measurement_type=measurement_type,
                instrument=instrument,
                sampling_period=sampling_period,
            )

    def read_obspack(
        self,
        data_filepath: Union[str, Path],
        site: str,
        inlet: str,
        measurement_type: str,
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
    ):
        """Read NOAA ObsPack NetCDF files

        Args:
            data_filepath: Path to file
            site: Three letter site code
            inlet: Inlet height, if no height use measurement type e.g. flask
            measurement_type: One of flask, insity or pfp
            instrument: Instrument name
            sampling_period: Sampling period
        Returns:
            dict: Dictionary of results
        """
        import xarray as xr
        from openghg.util import clean_string

        # from numpy import array as np_array

        # from openghg.processing import assign_attributes

        valid_types = ("flask", "insitu", "pfp")

        if measurement_type not in valid_types:
            raise ValueError(f"measurement_type must be one of {valid_types}")

        obspack_ds = xr.open_dataset(data_filepath)
        orig_attrs = obspack_ds.attrs

        # TODO - the simplest way of getting a clean Dataset in the form we want
        # seems to be to go to a pandas Dataframe and back, open to suggestions on this.
        # GJ - 2021-04-15
        df = obspack_ds.to_dataframe()
        df = df.set_index(df["time"])
        df = df[~df.index.duplicated(keep="first")]

        wanted = ["value", "value_unc", "nvalue", "value_std_dev"]
        to_extract = [x for x in wanted if x in df]

        if not to_extract:
            raise ValueError(
                f"No valid data columns found in converted DataFrame. We expect the following data variables in the passed NetCDF: {wanted}"
            )

        df = df[to_extract]

        if not df.index.is_monotonic_increasing:
            df = df.sort_index()

        processed_ds = df.to_xarray()
        # TODO - need to choose which keys we want to keep
        # GJ - 2021-04-15
        processed_ds.attrs = orig_attrs

        species = clean_string(obspack_ds.attrs["dataset_parameter"])
        network = "NOAA"

        metadata = {}
        metadata["site"] = site
        metadata["inlet"] = inlet
        metadata["network"] = network
        metadata["measurement_type"] = measurement_type
        metadata["species"] = species

        if instrument is not None:
            metadata["instrument"] = instrument

        if sampling_period is not None:
            metadata["sampling_period"] = sampling_period

        data = {}
        data[species] = {"data": processed_ds, "metadata": metadata}

        # TODO - how do we want to handle the CF compliance for the ObsPack files?
        # GJ - 2021-04-14
        # data = assign_attributes(data=data, site=site, network=network)

        return data

    def read_raw_file(
        self,
        data_filepath: Union[str, Path],
        site: str,
        inlet: str,
        measurement_type: str,
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
    ) -> Dict:
        """Reads NOAA data files and returns a dictionary of processed
        data and metadata.

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

        species = filename.split("_")[0].lower()

        source_name = data_filepath.stem
        source_name = source_name.split("-")[0]

        gas_data = self.read_raw_data(
            data_filepath=data_filepath, inlet=inlet, species=species, measurement_type=measurement_type
        )

        gas_data = assign_attributes(data=gas_data, site=site, network="NOAA")

        return gas_data

    def read_raw_data(self, data_filepath: Path, species: str, inlet: str, measurement_type: Optional[str] = "flask") -> Dict:
        """Separates the gases stored in the dataframe in
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
        from openghg.util import compliant_string, read_header
        from pandas import read_csv, Timestamp

        header = read_header(filepath=data_filepath)

        column_names = header[-1][14:].split()

        def date_parser(year, month, day, hour, minute, second):
            return Timestamp(year, month, day, hour, minute, second)

        date_parsing = {"time": ["sample_year", "sample_month", "sample_day", "sample_hour", "sample_minute", "sample_seconds"]}

        data_types = {
            "sample_year": int,
            "sample_month": int,
            "sample_day": int,
            "sample_hour": int,
            "sample_minute": int,
            "sample_seconds": int,
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
                raise ValueError(
                    f"Mismatch between passed species ({passed_species}) and species read from data ({data_species})"
                )

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
        metadata["species"] = compliant_string(species)
        metadata["site"] = site
        metadata["measurement_type"] = measurement_type
        metadata["network"] = "NOAA"
        metadata["inlet"] = inlet

        combined_data[species.lower()] = {
            "metadata": metadata,
            "data": data,
            "attributes": site_attributes,
        }

        return combined_data
