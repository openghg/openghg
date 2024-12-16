from __future__ import annotations
from pathlib import Path
from typing import Any
import logging
from openghg.store.base import BaseStore
from openghg.util import load_standardise_parser, split_function_inputs

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

__all__ = ["EulerianModel"]


# TODO: Currently built around these keys but will probably need more unique distiguishers for different setups
# model name
# species
# date (start_date)
# ...
# setup (included as option for now)


class EulerianModel(BaseStore):
    """This class is used to process Eulerian model data"""

    _data_type = "eulerian_model"
    _root = "EulerianModel"
    _uuid = "63ff2365-3ba2-452a-a53d-110140805d06"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_file(
        self,
        filepath: str | Path,
        model: str,
        species: str,
        source_format: str = "openghg",
        start_date: str | None = None,
        end_date: str | None = None,
        setup: str | None = None,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        chunks: dict | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Read Eulerian model output

        Args:
            filepath: Path of Eulerian model species output
            model: Eulerian model name
            species: Species name
            source_format: Data format, for example openghg (internal format)
            start_date: Start date (inclusive) associated with model run
            end_date: End date (exclusive) associated with model run
            setup: Additional setup details for run
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
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
            optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        """
        # TODO: As written, this currently includes some light assumptions that we're dealing with GEOSChem SpeciesConc format.
        # May need to split out into multiple modules (like with ObsSurface) or into separate retrieve functions as needed.

        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        from openghg.store.spec import define_standardise_parsers
        from openghg.util import (
            clean_string,
            check_if_need_new_version,
        )

        model = clean_string(model)
        species = clean_string(species)
        start_date = clean_string(start_date)
        end_date = clean_string(end_date)
        setup = clean_string(setup)

        # Specify any additional metadata to be added
        additional_metadata = {}

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

        standardise_parsers = define_standardise_parsers()[self._data_type]

        try:
            source_format = standardise_parsers[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Loading parse
        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        # Get current parameter values and filter to only include function inputs
        fn_current_parameters = locals().copy()  # Make a copy of parameters passed to function
        fn_input_parameters = {key: fn_current_parameters[key] for key in fn_input_parameters}

        fn_input_parameters["filepath"] = filepath

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        # Call appropriate standardisation function with input parameters
        eulerian_model_data = parser_fn(**parser_input_parameters)

        # # TODO: Add schema and validate methods so this can be checked against expected format
        # for split_data in eulerian_model_data.values():

        #     em_data = split_data["data"]
        #     EulerianModel.validate_data(em_data)

        # Check to ensure no required keys are being passed through optional_metadata dict
        self.check_info_keys(optional_metadata)
        if optional_metadata is not None:
            additional_metadata.update(optional_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        model_data = self.update_metadata(
            eulerian_model_data, additional_input_parameters, additional_metadata
        )

        data_type = "eulerian_model"
        datasource_uuids = self.assign_data(
            data=model_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
            filters=filters,
        )

        # TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
        # update_keys = ["start_date", "end_date", "latest_version"]
        # model_data = update_metadata(
        #     data_dict=model_data, uuid_dict=datasource_uuids, update_keys=update_keys
        # )

        # em_store.add_datasources(
        #     uuids=datasource_uuids, data=model_data, metastore=metastore, update_keys=update_keys
        # )

        logger.info(f"Completed processing: {filepath.name}.")

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids
