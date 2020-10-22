from typing import Dict, List, Optional, Union, Tuple
from pathlib import Path
from pandas import DataFrame

__all__ = ["GCWERKS"]


class GCWERKS:
    """ Class for processing ICOS data

    """

    def __init__(self):
        from openghg.util import load_hugs_json

        self._sampling_period = 0
        # Load site data
        data = load_hugs_json(filename="process_gcwerks_parameters.json")
        self._gc_params = data["GCWERKS"]
        # Site codes for inlet readings
        self._site_codes = load_hugs_json(filename="site_codes.json")

    def read_file(
        self,
        data_filepath: Union[str, Path],
        precision_filepath: Union[str, Path],
        site: Optional[str] = None,
        instrument: Optional[str] = None,
        network: Optional[str] = None,
    ) -> Dict:
        """ Reads a GC data file by creating a GC object and associated datasources

            Args:
                data_filepath: Path of data file
                precision_filepath: Path of precision file
                site: Three letter code or name for site
                instrument: Instrument name
                network: Network name
            Returns:
                dict: Dictionary of source_name : UUIDs
        """
        from pathlib import Path
        from openghg.processing import assign_attributes
        from openghg.util import is_number
        import re

        data_filepath = Path(data_filepath)

        if site is None:
            # Read from the filename
            site_name = re.findall(r"[\w']+", data_filepath.stem)[0]
            site = self.get_site_code(site_name)

        # We need to have the 3 character site code here
        if len(site) != 3:
            site = self.get_site_code(site)

        # Try and find the instrument name in the filename
        if instrument is None:
            # Get the first part of the filename
            # Example filename: capegrim-medusa.18.C
            instrument = re.findall(r"[\w']+", str(data_filepath.name))[1]

            if is_number(instrument):
                # has picked out the year, rather than the instrument. Default to GCMD for this type of file
                instrument = "GCMD"

        if network is None:
            network = "NA"

        gas_data = self.read_data(
            data_filepath=data_filepath, precision_filepath=precision_filepath, site=site, instrument=instrument, network=network
        )

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data

    def read_data(self, data_filepath: Path, precision_filepath: Path, site: str, instrument: str, network: str) -> Dict:
        """ Read data from the data and precision files

            Args:
                data_filepath: Path of data file
                precision_filepath: Path of precision file
                site: Name of site
                instrument: Instrument name
                network: Network name
            Returns:
                dict: Dictionary of gas data keyed by species
        """
        from datetime import datetime
        from pandas import read_csv
        from pandas import Timedelta as pd_Timedelta

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

        # This metadata will be added to when species are split and attributes are written
        metadata = {"instrument": instrument, "site": site, "network": network}

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
                data[gas_name + " status_flag"] = (data[column].str[0] != "-").astype(int)
                data[gas_name + " integration_flag"] = (data[column].str[1] != "-").astype(int)

                col_shift = 4
                units[gas_name] = header.iloc[1, col_loc + col_shift]
                scale[gas_name] = header.iloc[0, col_loc + col_shift]

                if units[gas_name] == "--":
                    units[gas_name] = "NA"

                if scale[gas_name] == "--":
                    scale[gas_name] = "NA"

                # Note - have removed this due to some files such as those from capegrim
                # not having units or scales in the data
                # Ensure the units and scale have been read in correctly
                # Have this in case the column shift between the header and data changes
                # if units[gas_name] == "--" or scale[gas_name] == "--":
                #     raise ValueError(
                #         f"Error reading units and scale, ensure columns are correct \
                #         between header and dataframe. File: {data_filepath}, species: {gas_name}"
                #     )

                species.append(gas_name)

        # Rename columns to include the gas this flag represents
        data = data.rename(columns=columns_renamed, inplace=False)

        precision, precision_species = self.read_precision(filepath=precision_filepath)

        for sp in species:
            precision_index = precision_species.index(sp) * 2 + 1
            data[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(data, method="pad")

        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        self._sampling_period = self.get_precision(instrument)

        data["new_time"] = data.index - pd_Timedelta(seconds=self._sampling_period / 2.0)

        data = data.set_index("new_time", inplace=False, drop=True)
        data.index.name = "time"

        gas_data = self.split_species(
            data=data, site=site, species=species, instrument=instrument, metadata=metadata, units=units, scale=scale,
        )

        return gas_data

    def read_precision(self, filepath: Path) -> Tuple[DataFrame, List]:
        """ Read GC precision file

            Args:
                filepath: Path of precision file
            Returns:
                tuple (Pandas.DataFrame, list): Precision DataFrame and list of species in
                precision data
        """
        from pandas import read_csv
        from datetime import datetime

        # Function for parsing datetime
        def prec_date_parser(date):
            return datetime.strptime(date, "%y%m%d")

        # Read precision species
        precision_header = read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

        precision_species = precision_header.values[0][1:].tolist()

        precision = read_csv(
            filepath, skiprows=5, header=None, sep=r"\s+", index_col=0, parse_dates=[0], date_parser=prec_date_parser,
        )

        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species

    def split_species(
        self, data: DataFrame, site: str, instrument: str, species: str, metadata: Dict, units: Dict, scale: Dict
    ) -> Dict:
        """ Splits the species into separate dataframe into sections to be stored within individual Datasources

            Args:
                data: DataFrame of raw data
                site: Name of site from which this data originates
                instrument: Name of instrument
                species: List of species contained in data
                metadata: Dictionary of metadata
                units: Dictionary of units for each species
                scale: Dictionary of scales for each species
            Returns:
                dict: Dataframe of gas data and metadata
        """
        from fnmatch import fnmatch

        # Read inlets from the parameters dictionary
        expected_inlets = self.get_inlets(site_code=site)
        # Get the inlets in the dataframe
        try:
            data_inlets = data["Inlet"].unique().tolist()
        except KeyError:
            raise KeyError(
                "Unable to read inlets from data, please ensure this data is of the GC \
                                    type expected by this processing module"
            )

        # For now just add air to the expected inlets
        expected_inlets.append("air")

        matching_inlets = [data_inlet for data_inlet in data_inlets for inlet in expected_inlets if fnmatch(data_inlet, inlet)]

        if not matching_inlets:
            raise ValueError(
                "Inlet mismatch - please ensure correct site is selected. \
                                Mismatch between inlet in data and inlet in parameters file."
            )

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
                    spec_data = data[[spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]]
                    spec_data = spec_data.dropna(axis="index", how="any")
                elif "date" in inlet:
                    dates = inlet.split("_")[1:]
                    slice_dict = {"time": slice(dates[0], dates[1])}
                    data_sliced = data.loc(slice_dict)
                    spec_data = data_sliced[
                        [spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")
                else:
                    # Take only data for this inlet from the dataframe
                    inlet_data = data.loc[data["Inlet"] == inlet]
                    spec_data = inlet_data[
                        [spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")

                attributes = self.get_site_attributes(site=site, inlet=inlet, instrument=instrument)

                # We want an xarray Dataset
                spec_data = spec_data.to_xarray()

                combined_data[spec] = {}
                combined_data[spec]["metadata"] = spec_metadata
                combined_data[spec]["data"] = spec_data
                combined_data[spec]["attributes"] = attributes

        return combined_data

    def is_valid_instrument(self, instrument: str) -> bool:
        """ Check if the instrument string passed is valid

            Returns:
                bool: True if valid
        """
        valid_instruments = self._gc_params["suffix_to_instrument"].keys()

        return instrument.lower() in valid_instruments

    def get_precision(self, instrument: str) -> int:
        """ Get the precision of the instrument in seconds

            Args:
                instrument (str): Instrument name
            Returns:
                int: Precision of instrument in seconds

        """
        try:
            sampling_period = self._gc_params["sampling_period"][instrument]
        except KeyError:
            raise ValueError(
                f"Invalid instrument: {instrument}\nPlease select one of {self._gc_params['sampling_period'].keys()}\n"
            )

        return sampling_period

    def get_inlets(self, site_code: str) -> List:
        """ Get the inlets used at this site

            Args:
                site (str): Site of datasources
            Returns:
                list: List of inlets
        """
        return self._gc_params[site_code]["inlets"]

    def get_site_code(self, site: str) -> str:
        """ Get the site code

            Args:
                site (str): Name of site
            Returns:
                str: Site code
        """
        try:
            site_code = self._site_codes["name_code"][site.lower()]
        except KeyError:
            raise KeyError(f"Site: {site} not recognized")

        return site_code

    def get_site_attributes(self, site: str, inlet: str, instrument: str) -> Dict:
        """ Gets the site specific attributes for writing to Datsets

            Args:
                site (str): Site name
                inlet (str): Inlet (example: 108m)
            Returns:
                dict: Dictionary of attributes
        """
        attributes = self._gc_params[site.upper()]["global_attributes"]

        attributes["inlet_height_magl"] = inlet
        try:
            attributes["comment"] = self._gc_params["comment"][instrument]
        except KeyError:
            valid_instruments = list(self._gc_params["comment"].keys())
            raise KeyError(f"Invalid instrument passed, valid instruments : {valid_instruments}")

        return attributes
