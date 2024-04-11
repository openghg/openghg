from __future__ import annotations

import logging
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
from xarray import Dataset
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore

__all__ = ["OneDTimeseries"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class OneDTimeseries(BaseStore):
    """This class is used to process ond dimension timeseries data"""

    _data_type = "OneDTimeseries"

    # New uuid is generated using the package - Delete comment in future
    """ _root = "OneDTimeseries"
    _uuid = "099b597b-0598-4efa-87dd-472dfe027f5d8"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"""

    def read_data(self, binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            return self.read_file(filepath=filepath, **metadata)

    def read_file(
        self,
        filepath: Union[str, Path],
        species: str,
        domain: str,
        source: str,
        database: Optional[str] = None,
        database_version: Optional[str] = None,
        model: Optional[str] = None,
        source_format: str = "crf",
        chunks: Optional[Dict] = None,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> dict:
        """Read one dimension timeseries file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
            domain: Region for boundary conditions
            source: Flux / Emissions source
            database: Name of database source for this input (if relevant)
            database_version: Name of database version (if relevant)
            model: Model name (if relevant)
            source_format : Type of data being input e.g. openghg (internal format)
            period: Period of measurements. Only needed if this can not be inferred from the time coords
            If specified, should be one of:
                - "yearly", "monthly"
                - suitable pandas Offset Alias
                - tuple of (value, unit) as would be passed to pandas.Timedelta function
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
            continuous: Whether time stamps have to be continuous.
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "combine" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
                - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
                - "n" / "no" - Allow current data to updated / deleted
            overwrite: Deprecated. This will use options for if_exists="new".
            force: Force adding of data even if this is identical to data stored.
            compressor: A custom compressor to use. If None, this will default to
                `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
                See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.      Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from openghg.types import OneD_types

        from openghg.util import (
            clean_string,
            load_oned_parser,
            check_if_need_new_version,
        )

        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure new version will be created by default if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        filepath = Path(filepath)

        try:
            source_format = OneD_types[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_oned_parser(source_format=source_format)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        # Define parameters to pass to the parser function
        # TODO: Update this to match against inputs for parser function.
        param = {
            "filepath": filepath,
            "species": species,
            "domain": domain,
            "source": source,
            "data_type": "OneDTimeseries",
            "chunks": chunks,
        }

        optional_keywords: dict[Any, Any] = {
            "database": database,
            "database_version": database_version,
            "model": model,
        }

        signature = inspect.signature(parser_fn)
        fn_accepted_parameters = [param.name for param in signature.parameters.values()]

        input_parameters: dict[Any, Any] = param.copy()

        # Checks if optional parameters are present in function call and includes them else ignores its inclusion in input_parameters.
        for param, param_value in optional_keywords.items():
            if param in fn_accepted_parameters:
                input_parameters[param] = param_value
            else:
                logger.warning(
                    f"Input: '{param}' (value: {param_value}) is not being used as part of the standardisation process."
                    f"This is not accepted by the current standardisation function: {parser_fn}"
                )

        oned_data = parser_fn(**input_parameters)

        # Checking against expected format for Flux
        for split_data in oned_data.values():
            em_data = split_data["data"]
            OneDTimeseries.validate_data(em_data)

        min_required = ["species", "source", "domain"]
        for key, value in optional_keywords.items():
            if value is not None:
                min_required.append(key)

        required = tuple(min_required)

        data_type = "OneDTimeseries"
        datasource_uuids = self.assign_data(
            data=oned_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            required_keys=required,
            compressor=compressor,
            filters=filters,
        )

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    # TODO: Delete below comments after complete agreement on development.

    #     if filepath.suffix.endswith(".nc"):
    #         oned_data = open_dataset(filepath)
    #     elif filepath.suffix.endswith(".xlsx") or (".csv"):
    #         # TODO: Determine the index, and values to be fetched from the csv files inorder to be converted into xarray dataset compatible for further operations
    #         oned_data = pd.read_csv(filepath)
    #         oned_data = Dataset.from_dataframe(oned_data)
    #     # Some attributes are numpy types we can't serialise to JSON so convert them
    #     # to their native types here
    #     attrs = {}
    #     for key, value in oned_data.attrs.items():
    #         try:
    #             attrs[key] = value.item()
    #         except AttributeError:
    #             attrs[key] = value

    #     author_name = "OpenGHG Cloud"
    #     oned_data.attrs["author"] = author_name

    #     metadata = {}
    #     metadata.update(attrs)

    #     metadata["species"] = species
    #     metadata["domain"] = domain
    #     metadata["author"] = author_name
    #     metadata["processed"] = str(timestamp_now())

    #     # Check if time has 0-dimensions and, if so, expand this so time is 1D
    #     if "time" in oned_data.coords:
    #         oned_data = update_zero_dim(oned_data, dim="time")

    #     # Currently ACRG boundary conditions are split by month or year
    #     oneD_time = oned_data["time"]

    #     start_date, end_date, period_str = infer_date_range(
    #         oneD_time, filepath=filepath, period=period, continuous=continuous
    #     )

    #     # Checking against expected format for boundary conditions
    #     OneDTimeseries.validate_data(oned_data)
    #     data_type = "oned_timeseries"

    #     metadata["start_date"] = str(start_date)
    #     metadata["end_date"] = str(end_date)
    #     metadata["data_type"] = data_type

    #     metadata["input_filename"] = filepath.name

    #     metadata["time_period"] = period_str

    #     key = "_".join((species, domain))

    #     oneD_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
    #     oneD_data[key]["data"] = oned_data
    #     oneD_data[key]["metadata"] = metadata

    #     required_keys = ("species", "domain")

    #     # This performs the lookup and assignment of data to new or
    #     # exisiting Datasources
    #     data_type = "oned_timeseries"
    #     datasource_uuids = self.assign_data(
    #         data=oneD_data,
    #         if_exists=if_exists,
    #         new_version=new_version,
    #         data_type=data_type,
    #         required_keys=required_keys,
    #         compressor=compressor,
    #         filters=filters,
    #     )

    #     # Record the file hash in case we see this file again
    #     self._file_hashes[file_hash] = filepath.name

    #     return datasource_uuids

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
        Validate input data against BoundaryConditions schema - definition from
        BoundaryConditions.schema() method.

        Args:
            data : xarray Dataset in expected format

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the BoundaryConditions schema.
        """
        data_schema = OneDTimeseries.schema()
        data_schema.validate_data(data)

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for one dimensional timeseries(OneDTimeseries) Dataset.

        Includes observation for each time of the defined domain:
            - "Obs"
                - expected dimensions: ("time")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for OneDTimeseries.
        """
        from openghg.store import DataSchema

        data_vars: Dict[str, Tuple[str, ...]] = {"Obs": ("time", "")}
        dtypes = {
            "time": np.datetime64,
            "Obs": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
