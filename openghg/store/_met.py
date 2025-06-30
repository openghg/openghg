import logging
from pathlib import Path
from typing import Any
import numpy as np
from xarray import Dataset


from openghg.util import (
    clean_string,
    load_standardise_parser,
    split_function_inputs,
    check_if_need_new_version,
)
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.store.spec import define_standardise_parsers


__all__ = ["Met"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Met(BaseStore):
    """ """

    _data_type = "met"
    _root = "Met"
    _uuid = "dbb725a1-4102-4804-b732-9e2159fe04f1"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_file(
        self,
        filepath: str | Path,
        site: str,
        network: str,
        met_source: str,
        source_format: str,
        if_exists: str = "auto",
        save_current: str = "auto",
        force: bool = False,
        chunks: dict | None = None,
        compressor: Any | None = None,
    ):
        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        # Formatting inputs
        site = clean_string(site)
        network = clean_string(network)
        met_source = clean_string(met_source)

        # Finding appropriate parser based on data_type and source_format
        standardise_parsers = define_standardise_parsers()[self._data_type]

        try:
            source_format = standardise_parsers[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)

        # Making sure new version will be created by default if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        # Checking hashes for files previously added to object store
        filepath = Path(filepath)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return [{}]

        filepath = next(iter(unseen_hashes.values()))

        # Setting chunks to default if needed
        if chunks is None:
            chunks = {}

        # Get current parameter values and filter to only include function inputs
        fn_current_parameters = locals().copy()  # Make a copy of parameters passed to function
        fn_input_parameters = {key: fn_current_parameters[key] for key in fn_input_parameters}

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        data = parser_fn(**parser_input_parameters)

        # Validate parsed data to make sure this conforms to internal standard
        # TODO: Add more deteils to schema() method for this
        for entry_to_store in data:
            Met.validate_data(entry_to_store.data)

        # # TODO: Merge devel and update this to info_metadata
        # # Check to ensure no required keys are being passed through optional_metadata dict
        # self.check_info_keys(optional_metadata)
        # if optional_metadata is not None:
        #     additional_metadata.update(optional_metadata)
        additional_metadata = {}

        # Mop up and add additional keys to metadata which weren't passed to the parser
        data = self.update_metadata(data, additional_input_parameters, additional_metadata)

        data_type = self._data_type
        datasource_uuids = self.assign_data(
            data=data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
        )

        logger.info(f"Completed processing: {filepath.name}.")

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """ """
        data_schema = Met.schema()
        data_schema.validate_data(data)

    @staticmethod
    def schema() -> DataSchema:
        """ """
        # TODO: Add details of expected format for internal data
        data_vars = {}
        dtypes = {
            "lat": np.floating,  # Covers np.float16, np.float32, np.float64 types
            "lon": np.floating,
            "time": np.datetime64,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
