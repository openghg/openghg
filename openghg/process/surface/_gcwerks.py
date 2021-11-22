from typing import DefaultDict, Dict, List, Optional, Union, Tuple
from pathlib import Path
from pandas import DataFrame

__all__ = ["GCWERKS"]


class GCWERKS:
    """Class for retrieve GCWERKS data"""

    def __init__(self) -> None:
        from openghg.util import load_json

        # Load site data
        data = load_json(filename="process_gcwerks_parameters.json")
        self._gc_params = data["GCWERKS"]
        # Build the name to code dictionary to check check
        # passed metadata on read
        self._name_to_code = {}

        for site_code, value in self._gc_params["sites"].items():
            try:
                name = value["gcwerks_site_name"]
                self._name_to_code[name] = site_code
            except KeyError:
                pass

    def find_files(self, data_path: Union[str, Path], skip_str: Union[str, List[str]] = "sf6") -> List[Tuple[Path, Path]]:
        """A helper file to find GCWERKS data and precisions file in a given folder.
        It searches for .C files of the format macehead.19.C, looks for a precisions file
        named macehead.19.precions.C and if it exists creates a tuple for these files.

        Please note the limited scope of this function, it will only work with
        files that are named in the correct pattern.

        Args:
            data_path: Folder path to search
            skip_str: String or list of strings, if found in filename these files are skipped
        Returns:
            list: List of tuples
        """
        import re
        from pathlib import Path

        data_path = Path(data_path)

        files = data_path.glob("*.C")

        if not isinstance(skip_str, list):
            skip_str = [skip_str]

        data_regex = re.compile(r"[\w'-]+\.\d+.C")

        data_precision_tuples = []
        for file in files:
            data_match = data_regex.match(file.name)

            if data_match:
                prec_filepath = data_path / Path(Path(file).stem + ".precisions.C")
                data_filepath = data_path / data_match.group()

                if any(s in data_match.group() for s in skip_str):
                    continue

                if prec_filepath.exists():
                    data_precision_tuples.append((data_filepath, prec_filepath))

        data_precision_tuples.sort()

        return data_precision_tuples

    def read_file(
        self,
        data_filepath: Union[str, Path],
        precision_filepath: Union[str, Path],
        site: str,
        network: str,
        inlet: Optional[str] = None,
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
        measurement_type: Optional[str] = None,
    ) -> Dict:
        """Reads a GC data file by creating a GC object and associated datasources

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
        from openghg.retrieve import assign_attributes
        from openghg.util import valid_site, clean_string

        data_filepath = Path(data_filepath)
        precision_filepath = Path(precision_filepath)

        site = clean_string(site)
        network = clean_string(network)
        # We don't currently do anything with inlet here as it's always read from data
        # or taken from process_gcwerks_parameters.json
        if inlet is not None:
            inlet = clean_string(inlet)
        if instrument is not None:
            instrument = clean_string(instrument)

        if not valid_site(site):
            raise ValueError(f"Invalid site {site} passed.")

        # Check if the site code passed matches that read from the filename
        site = self.check_site(filepath=data_filepath, site_code=site)

        # If we're not passed the instrument name and we can't find it raise an error
        if instrument is None:
            instrument = self.check_instrument(filepath=data_filepath, should_raise=True)
        else:
            fname_instrument = self.check_instrument(filepath=data_filepath, should_raise=False)

            if fname_instrument is not None and instrument != fname_instrument:
                raise ValueError(
                    f"Mismatch between instrument passed as argument {instrument} and instrument read from filename {fname_instrument}"
                )

        instrument = str(instrument)

        gas_data = self.read_data(
            data_filepath=data_filepath,
            precision_filepath=precision_filepath,
            site=site,
            instrument=instrument,
            network=network,
            sampling_period=sampling_period,
        )

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data

    def check_site(self, filepath: Path, site_code: str) -> str:
        """Check if the site passed in matches that in the filename

        Args:
            filepath: Path to data file
            site: Site code
        Returns:
            str: Site code
        """
        from re import findall

        site_code = site_code.lower()
        site_name = findall(r"[\w']+", str(filepath.name))[0].lower()

        if len(site_code) > 3:
            raise ValueError("Please pass in a 3 letter site code as the site argument.")

        try:
            confirmed_code = self._name_to_code[site_name].lower()
        except KeyError:
            raise ValueError(f"Cannot match {site_name} to a site code.")

        if site_code != confirmed_code:
            raise ValueError(f"Mismatch between code reasd from filename: {confirmed_code} and that given: {site_code}")

        return site_code

    def check_instrument(self, filepath: Path, should_raise: bool = False) -> Union[str, None]:
        """Ensure we have the correct instrument or translate an instrument
        suffix to an instrument name.

        Args:
            instrument_suffix: Instrument suffix such as md
            should_raise: Should we raise if we can't find a valid instrument
        Returns:
            str: Instrument name
        """
        from re import findall

        instrument: str = findall(r"[\w']+", str(filepath.name))[1].lower()
        try:
            if instrument in self._gc_params["instruments"]:
                return instrument
            else:
                try:
                    instrument = self._gc_params["suffix_to_instrument"][instrument]
                except KeyError:
                    if "medusa" in instrument:
                        instrument = "medusa"
                    else:
                        raise KeyError(f"Invalid instrument {instrument}")
        except KeyError:
            if should_raise:
                raise
            else:
                return None

        return instrument

    def read_data(
        self,
        data_filepath: Path,
        precision_filepath: Path,
        site: str,
        instrument: str,
        network: str,
        sampling_period: Optional[str] = None,
    ) -> Dict:
        """Read data from the data and precision files

        Args:
            data_filepath: Path of data file
            precision_filepath: Path of precision file
            site: Name of site
            instrument: Instrument name
            network: Network name
            sampling_period: Period over which the measurement was samplied.
        Returns:
            dict: Dictionary of gas data keyed by species
        """
        from datetime import datetime
        from pandas import read_csv
        from pandas import Timedelta as pd_Timedelta

        # Read header
        header = read_csv(data_filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

        # Create a function to parse the datetime in the data file
        def parser(date: str) -> datetime:
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

        if data.empty:
            raise ValueError("Cannot process empty file.")

        data.index.name = "Datetime"

        # This metadata will be added to when species are split and attributes are written
        metadata: Dict[str, str] = {"instrument": instrument, "site": site, "network": network}

        extracted_sampling_period = self.get_precision(instrument)
        metadata["sampling_period"] = str(extracted_sampling_period)

        if sampling_period is not None:
            # Compare input to definition within json file
            file_sampling_period = pd_Timedelta(seconds=extracted_sampling_period)
            comparison_seconds = abs(sampling_period - file_sampling_period).total_seconds()
            tolerance_seconds = 1

            if comparison_seconds > tolerance_seconds:
                raise ValueError(
                    f"Input sampling period {sampling_period} does not match to value "
                    f"extracted from the file name of {metadata['sampling_period']} seconds."
                )

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

                species.append(gas_name)

        # Rename columns to include the gas this flag represents
        data = data.rename(columns=columns_renamed, inplace=False)

        precision, precision_species = self.read_precision(filepath=precision_filepath)

        # Check if the index is sorted
        if not precision.index.is_monotonic_increasing:
            precision = precision.sort_index()

        for sp in species:
            try:
                precision_index = precision_species.index(sp) * 2 + 1
            except ValueError:
                raise ValueError(f"Cannot find {sp} in precisions file.")

            data[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(data, method="pad")

        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        data["new_time"] = data.index - pd_Timedelta(seconds=int(metadata["sampling_period"]) / 2.0)

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

    def read_precision(self, filepath: Path) -> Tuple[DataFrame, List]:
        """Read GC precision file

        Args:
            filepath: Path of precision file
        Returns:
            tuple (Pandas.DataFrame, list): Precision DataFrame and list of species in
            precision data
        """
        from pandas import read_csv
        from datetime import datetime

        # Function for parsing datetime
        def prec_date_parser(date: str) -> datetime:
            return datetime.strptime(date, "%y%m%d")

        # Read precision species
        precision_header = read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

        precision_species = precision_header.values[0][1:].tolist()

        precision = read_csv(
            filepath,
            skiprows=5,
            header=None,
            sep=r"\s+",
            index_col=0,
            parse_dates=[0],
            date_parser=prec_date_parser,
        )

        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species

    def split_species(
        self, data: DataFrame, site: str, instrument: str, species: List, metadata: Dict, units: Dict, scale: Dict
    ) -> Dict:
        """Splits the species into separate dataframe into sections to be stored within individual Datasources

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
        from collections import defaultdict
        from fnmatch import fnmatch
        from openghg.util import clean_string

        # Read inlets from the parameters
        expected_inlets = self.get_inlets(site_code=site)

        try:
            data_inlets = data["Inlet"].unique().tolist()
        except KeyError:
            raise KeyError(
                "Unable to read inlets from data, please ensure this data is of the GC type expected by this retrieve module"
            )

        combined_data: DefaultDict[str, Dict] = defaultdict(dict)

        for spec in species:
            # Skip this species if the data is all NaNs
            if data[spec].isnull().all():
                continue

            # Here inlet is the inlet in the data and inlet_label is the label we want to use as metadata
            for inlet, inlet_label in expected_inlets.items():
                # Create a copy of metadata for local modification
                spec_metadata = metadata.copy()
                spec_metadata["species"] = clean_string(spec)
                spec_metadata["units"] = units[spec]
                spec_metadata["scale"] = scale[spec]

                # If we've only got a single inlet
                if inlet == "any" or inlet == "air":
                    spec_data = data[[spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]]
                    spec_data = spec_data.dropna(axis="index", how="any")
                    spec_metadata["inlet"] = inlet_label
                elif "date" in inlet:
                    dates = inlet.split("_")[1:]
                    data_sliced = data.loc[dates[0] : dates[1]]

                    spec_data = data_sliced[
                        [spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]
                    ]
                    spec_data = spec_data.dropna(axis="index", how="any")
                    spec_metadata["inlet"] = inlet_label
                else:
                    # Find the inlet
                    matching_inlets = [i for i in data_inlets if fnmatch(i, inlet)]

                    if not matching_inlets:
                        continue

                    # Only set the label in metadata when we have the correct label
                    spec_metadata["inlet"] = inlet_label
                    # There should only be one matching label
                    select_inlet = matching_inlets[0]
                    # Take only data for this inlet from the dataframe
                    inlet_data = data.loc[data["Inlet"] == select_inlet]

                    spec_data = inlet_data[
                        [spec, spec + " repeatability", spec + " status_flag", spec + " integration_flag", "Inlet"]
                    ]

                    spec_data = spec_data.dropna(axis="index", how="any")

                # Now we drop the inlet column
                spec_data = spec_data.drop("Inlet", axis="columns")

                # Check that the Dataframe has something in it
                if spec_data.empty:
                    continue

                attributes = self.get_site_attributes(site=site, inlet=inlet_label, instrument=instrument).copy()

                # We want an xarray Dataset
                spec_data = spec_data.to_xarray()
                # A cleaned species label
                comp_species = clean_string(spec)

                # Rename variables so they have lowercase and alphanumeric names
                to_rename = {}
                for var in spec_data.variables:
                    if spec in var:
                        new_name = var.replace(spec, comp_species)
                        to_rename[var] = new_name

                spec_data = spec_data.rename(to_rename)

                # As a single species may have measurements from multiple inlets we
                # use the species and inlet as a key
                data_key = f"{comp_species}_{inlet_label}"

                combined_data[data_key]["metadata"] = spec_metadata
                combined_data[data_key]["data"] = spec_data
                combined_data[data_key]["attributes"] = attributes

        return combined_data

    def get_precision(self, instrument: str) -> str:
        """Process the suffix from the filename to get the correct instrument name
        then retrieve the precision of that instrument.

        Args:
            instrument: Instrument name
        Returns:
            int: Precision of instrument in seconds
        """
        instrument = instrument.lower()
        try:
            sampling_period = str(self._gc_params["sampling_period"][instrument])
        except KeyError:
            raise ValueError(
                f"Invalid instrument: {instrument}\nPlease select one of {self._gc_params['sampling_period'].keys()}\n"
            )

        return sampling_period

    def get_inlets(self, site_code: str) -> Dict:
        """Get the inlets we expect to be at this site and create a
        mapping dictionary so we get consistent labelling.

        Args:
            site: Site code
        Returns:
            dict: Mapping dictionary of inlet and required inlet label
        """
        site = site_code.upper()
        site_params = self._gc_params["sites"]

        # Create a mapping of inlet to match to the inlet label
        inlets = site_params[site]["inlets"]
        try:
            inlet_labels = site_params[site]["inlet_label"]
        except KeyError:
            inlet_labels = inlets

        mapping_dict = {k: v for k, v in zip(inlets, inlet_labels)}

        return mapping_dict

    def get_site_attributes(self, site: str, inlet: str, instrument: str) -> Dict[str, str]:
        """Gets the site specific attributes for writing to Datsets

        Args:
            site: Site code
            inlet: Inlet (example: 108m)
        Returns:
            dict: Dictionary of attributes
        """
        site = site.upper()
        instrument = instrument.lower()

        attributes: Dict[str, str] = self._gc_params["sites"][site]["global_attributes"]

        attributes["inlet_height_magl"] = inlet
        try:
            attributes["comment"] = self._gc_params["comment"][instrument]
        except KeyError:
            valid_instruments = list(self._gc_params["comment"].keys())
            raise KeyError(f"Invalid instrument {instrument} passed, valid instruments : {valid_instruments}")

        return attributes
