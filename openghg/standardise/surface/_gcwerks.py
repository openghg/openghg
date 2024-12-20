from pathlib import Path
from pandas import DataFrame

from openghg.standardise.meta import dataset_formatter
from openghg.types import optionalPathType


def find_files(data_path: str | Path, skip_str: str | list[str] = "sf6") -> list[tuple[Path, Path]]:
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
            filepath = data_path / data_match.group()

            if any(s in data_match.group() for s in skip_str):
                continue

            if prec_filepath.exists():
                data_precision_tuples.append((filepath, prec_filepath))

    data_precision_tuples.sort()

    return data_precision_tuples


def parse_gcwerks(
    filepath: str | Path,
    precision_filepath: str | Path,
    site: str,
    network: str,
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
) -> dict:
    """Reads a GC data file by creating a GC object and associated datasources

    Args:
        filepath: Path of data file
        precision_filepath: Path of precision file
        site: Three letter code or name for site
        instrument: Instrument name
        network: Network name
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        dict: Dictionary of source_name : UUIDs
    """
    from pathlib import Path

    from openghg.standardise.meta import assign_attributes
    from openghg.util import clean_string, load_internal_json

    filepath = Path(filepath)
    precision_filepath = Path(precision_filepath)

    # Do some setup for processing
    # Load site data
    gcwerks_data = load_internal_json(filename="process_gcwerks_parameters.json")
    gc_params = gcwerks_data["GCWERKS"]

    network = clean_string(network)
    # We don't currently do anything with inlet here as it's always read from data
    # or taken from process_gcwerks_parameters.json
    if inlet is not None:
        inlet = clean_string(inlet)
    if instrument is not None:
        instrument = clean_string(instrument)

    # Check if the site code passed matches that read from the filename
    site = _check_site(
        filepath=filepath,
        site_code=site,
        gc_params=gc_params,
    )

    # If we're not passed the instrument name and we can't find it raise an error
    if instrument is None:
        instrument = _check_instrument(filepath=filepath, gc_params=gc_params, should_raise=True)
    else:
        fname_instrument = _check_instrument(filepath=filepath, gc_params=gc_params, should_raise=False)

        if fname_instrument is not None and instrument != fname_instrument:
            raise ValueError(
                f"Mismatch between instrument passed as argument {instrument} and instrument read from filename {fname_instrument}"
            )

    instrument = str(instrument)

    gas_data = _read_data(
        filepath=filepath,
        precision_filepath=precision_filepath,
        site=site,
        instrument=instrument,
        network=network,
        sampling_period=sampling_period,
        gc_params=gc_params,
    )

    gas_data = dataset_formatter(data=gas_data)

    # Assign attributes to the data for CF compliant NetCDFs
    gas_data = assign_attributes(
        data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
    )

    return gas_data


def _check_site(filepath: Path, site_code: str, gc_params: dict) -> str:
    """Check if the site passed in matches that in the filename

    Args:
        filepath: Path to data file
        site: Site code
        gc_params: Dictionary of GCWERKS parameters
    Returns:
        str: Site code
    """
    from re import findall

    site_data = gc_params["sites"]
    name_code_conversion = {value["gcwerks_site_name"]: site_code for site_code, value in site_data.items()}

    site_code = site_code.lower()
    site_name = findall(r"[\w']+", str(filepath.name))[0].lower()

    if len(site_code) > 3:
        raise ValueError("Please pass in a 3 letter site code as the site argument.")

    try:
        confirmed_code = name_code_conversion[site_name].lower()
    except KeyError:
        raise ValueError(f"Cannot match {site_name} to a site code.")

    if site_code != confirmed_code:
        raise ValueError(
            f"Mismatch between code reasd from filename: {confirmed_code} and that given: {site_code}"
        )

    return site_code


def _check_instrument(filepath: Path, gc_params: dict, should_raise: bool = False) -> str | None:
    """Ensure we have the correct instrument or translate an instrument
    suffix to an instrument name.

    Args:
        instrument_suffix: Instrument suffix such as md
        should_raise: Should we raise if we can't find a valid instrument
        gc_params: GCWERKS parameters
    Returns:
        str: Instrument name
    """
    from re import findall

    instrument: str = findall(r"[\w']+", str(filepath.name))[1].lower()
    try:
        if instrument in gc_params["instruments"]:
            return instrument
        else:
            try:
                instrument = gc_params["suffix_to_instrument"][instrument]
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


def _read_data(
    filepath: Path,
    precision_filepath: Path,
    site: str,
    instrument: str,
    network: str,
    gc_params: dict,
    sampling_period: str | None = None,
) -> dict:
    """Read data from the data and precision files

    Args:
        filepath: Path of data file
        precision_filepath: Path of precision file
        site: Name of site
        instrument: Instrument name
        network: Network name
        gc_params: GCWERKS parameters
        sampling_period: Period over which the measurement was samplied.
    Returns:
        dict: Dictionary of gas data keyed by species
    """
    from pandas import Series
    from pandas import Timedelta as pd_Timedelta
    from pandas import read_csv

    # Read header
    header = read_csv(filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

    # Read the data in and automatically create a datetime column from the 5 columns
    # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
    data = read_csv(
        filepath,
        skiprows=4,
        sep=r"\s+",
        parse_dates={"Datetime": [1, 2, 3, 4, 5]},
        date_format="%Y %m %d %H %M",
        index_col="Datetime",
    )

    if data.empty:
        raise ValueError("Cannot process empty file.")

    # This metadata will be added to when species are split and attributes are written
    metadata: dict[str, str] = {
        "instrument": instrument,
        "site": site,
        "network": network,
    }

    extracted_sampling_period = _get_sampling_period(instrument=instrument, gc_params=gc_params)
    metadata["sampling_period"] = extracted_sampling_period

    if sampling_period is not None:
        # Compare input to definition within json file
        file_sampling_period_td = pd_Timedelta(seconds=float(extracted_sampling_period))
        sampling_period_td = pd_Timedelta(seconds=float(sampling_period))
        comparison_seconds = abs(sampling_period_td - file_sampling_period_td).total_seconds()
        tolerance_seconds = 1

        if comparison_seconds > tolerance_seconds:
            raise ValueError(
                f"Input sampling period {sampling_period} does not match to value "
                f"extracted from the file name of {metadata['sampling_period']} seconds."
            )

    units = {}
    scale = {}

    flag_columns: list[Series] = []
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

            # Create 2 new series based on the flag columns
            status_flag = (data[column].str[0] != "-").astype(int).rename(f"{gas_name} status_flag")
            integration_flag = (data[column].str[1] != "-").astype(int).rename(f"{gas_name} integration_flag")

            flag_columns.extend((status_flag, integration_flag))

            col_shift = 4
            units[gas_name] = header.iloc[1, col_loc + col_shift]
            scale[gas_name] = header.iloc[0, col_loc + col_shift]

            if units[gas_name] == "--":
                units[gas_name] = "NA"

            if scale[gas_name] == "--":
                scale[gas_name] = "NA"

            species.append(gas_name)

    data = data.join(flag_columns)
    # Rename columns to include the gas this flag represents
    data = data.rename(columns=columns_renamed, inplace=False)

    precision, precision_species = _read_precision(filepath=precision_filepath)

    # Check if the index is sorted
    if not precision.index.is_monotonic_increasing:
        precision = precision.sort_index()

    for sp in species:
        try:
            precision_index = precision_species.index(sp) * 2 + 1
        except ValueError:
            raise ValueError(f"Cannot find {sp} in precisions file.")

        data[sp + " repeatability"] = (
            precision[precision_index].astype(float).reindex_like(data, method="pad")
        )

    # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
    data["new_time"] = data.index - pd_Timedelta(seconds=int(metadata["sampling_period"]) / 2.0)

    data = data.set_index("new_time", inplace=False, drop=True)
    data.index.name = "time"

    gas_data = _split_species(
        data=data,
        site=site,
        species=species,
        instrument=instrument,
        metadata=metadata,
        units=units,
        scale=scale,
        gc_params=gc_params,
    )

    return gas_data


def _read_precision(filepath: Path) -> tuple[DataFrame, list]:
    """Read GC precision file

    Args:
        filepath: Path of precision file
    Returns:
        tuple (Pandas.DataFrame, list): Precision DataFrame and list of species in
        precision data
    """
    from pandas import read_csv

    # Read precision species
    precision_header = read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

    precision_species = precision_header.values[0][1:].tolist()

    precision = read_csv(
        filepath,
        skiprows=5,
        header=None,
        sep=r"\s+",
        index_col=0,
        parse_dates={"Datetime": [0]},
        date_format="%y%m%d",
    )

    # Drop any duplicates from the index
    precision = precision.loc[~precision.index.duplicated(keep="first")]

    return precision, precision_species


def _split_species(
    data: DataFrame,
    site: str,
    instrument: str,
    species: list,
    metadata: dict,
    units: dict,
    scale: dict,
    gc_params: dict,
) -> dict:
    """Splits the species into separate dataframe into sections to be stored within individual Datasources

    Args:
        data: DataFrame of raw data
        site: Name of site from which this data originates
        instrument: Name of instrument
        species: List of species contained in data
        metadata: Dictionary of metadata
        units: Dictionary of units for each species
        scale: Dictionary of scales for each species
        gc_params: GCWERKS parameter dictionary
    Returns:
        dict: Dataframe of gas data and metadata
    """
    from fnmatch import fnmatch

    from addict import Dict as aDict
    from openghg.util import format_inlet
    from openghg.standardise.meta import define_species_label

    # Read inlets from the parameters
    expected_inlets = _get_inlets(site_code=site, gc_params=gc_params)

    try:
        data_inlets = data["Inlet"].unique().tolist()
    except KeyError:
        raise KeyError(
            "Unable to read inlets from data, please ensure this data is of the GC type expected by this retrieve module"
        )

    combined_data = aDict()

    for spec in species:
        # Skip this species if the data is all NaNs
        if data[spec].isnull().all():
            continue

        # Here inlet is the inlet in the data and inlet_label is the label we want to use as metadata
        for inlet, inlet_label in expected_inlets.items():
            inlet_label = format_inlet(inlet_label)
            # Create a copy of metadata for local modification
            spec_metadata = metadata.copy()
            spec_metadata["units"] = units[spec]
            spec_metadata["calibration_scale"] = scale[spec]

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
                spec_metadata["inlet"] = inlet_label
            elif "date" in inlet:
                dates = inlet.split("_")[1:]
                data_sliced = data.loc[dates[0] : dates[1]]

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
                    [
                        spec,
                        spec + " repeatability",
                        spec + " status_flag",
                        spec + " integration_flag",
                        "Inlet",
                    ]
                ]

                spec_data = spec_data.dropna(axis="index", how="any")

            # Now we drop the inlet column
            spec_data = spec_data.drop("Inlet", axis="columns")

            # Check that the Dataframe has something in it
            if spec_data.empty:
                continue

            attributes = _get_site_attributes(
                site=site, inlet=inlet_label, instrument=instrument, gc_params=gc_params
            )
            attributes = attributes.copy()

            # We want an xarray Dataset
            spec_data = spec_data.to_xarray()

            # Create a standardised / cleaned species label
            comp_species = define_species_label(spec)[0]

            # Add the cleaned species name to the metadata and alternative name if present
            spec_metadata["species"] = comp_species
            spec_metadata["data_type"] = "surface"

            if comp_species != spec.lower() and comp_species != spec.upper():
                spec_metadata["species_alt"] = spec

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

    to_return: dict = combined_data.to_dict()

    return to_return


def _get_sampling_period(instrument: str, gc_params: dict) -> str:
    """Process the suffix from the filename to get the correct instrument name
    then retrieve the sampling period of that instrument from metadata.

    Args:
        instrument: Instrument name
    Returns:
        str: Precision of instrument in seconds
    """
    instrument = instrument.lower()
    try:
        sampling_period = str(gc_params["sampling_period"][instrument])
    except KeyError:
        raise ValueError(
            f"Invalid instrument: {instrument}\nPlease select one of {gc_params['sampling_period'].keys()}\n"
        )

    return sampling_period


def _get_inlets(site_code: str, gc_params: dict) -> dict:
    """Get the inlets we expect to be at this site and create a
    mapping dictionary so we get consistent labelling.

    Args:
        site: Site code
        gc_params: GCWERKS parameters
    Returns:
        dict: Mapping dictionary of inlet and required inlet label
    """
    site = site_code.upper()
    site_params = gc_params["sites"]

    # Create a mapping of inlet to match to the inlet label
    inlets = site_params[site]["inlets"]
    try:
        inlet_labels = site_params[site]["inlet_label"]
    except KeyError:
        inlet_labels = inlets

    mapping_dict = {k: v for k, v in zip(inlets, inlet_labels)}

    return mapping_dict


def _get_site_attributes(site: str, inlet: str, instrument: str, gc_params: dict) -> dict[str, str]:
    """Gets the site specific attributes for writing to Datsets

    Args:
        site: Site code
        inlet: Inlet height in metres
        instrument: Instrument name
        gc_params: GCWERKS parameters
    Returns:
        dict: Dictionary of attributes
    """
    from openghg.util import format_inlet

    site = site.upper()
    instrument = instrument.lower()

    attributes: dict[str, str] = gc_params["sites"][site]["global_attributes"]

    attributes["inlet_height_magl"] = format_inlet(inlet, key_name="inlet_height_magl")
    try:
        attributes["comment"] = gc_params["comment"][instrument]
    except KeyError:
        valid_instruments = list(gc_params["comment"].keys())
        raise KeyError(f"Invalid instrument {instrument} passed, valid instruments : {valid_instruments}")

    return attributes
