from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TYPE_CHECKING
import numpy as np
from xarray import Dataset

from openghg.util import align_lat_lon, load_standardise_parser, split_function_inputs, synonyms

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore
from openghg.store.spec import define_standardise_parsers

__all__ = ["BoundaryConditions"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _data_type = "boundary_conditions"
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(
        self,
        binary_data: bytes,
        metadata: dict,
        file_metadata: dict,
        source_format: str,
    ) -> dict | None:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
            source_format : Type of data being input e.g. openghg (internal format)

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

            return self.read_file(filepath=filepath, source_format=source_format, **metadata)

    def read_file(
        self,
        filepath: str | Path,
        species: str,
        bc_input: str,
        domain: str,
        source_format: str,
        period: str | tuple | None = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        chunks: dict | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Read boundary conditions file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
            bc_input: Input used to create boundary conditions. For example:
              - a model name such as "MOZART" or "CAMS"
              - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
            domain: Region for boundary conditions
            source_format : Type of data being input e.g. openghg (internal format)
            period: Period of measurements. Only needed if this can not be inferred from the time coords
                    If specified, should be one of:
                     - "yearly", "monthly"
                     - suitable pandas Offset Alias
                     - tuple of (value, unit) as would be passed to pandas.Timedelta function
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
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
            optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        Returns:
            dict: Dictionary of files processed and datasource UUIDs data assigned to
        """
        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        from openghg.util import (
            clean_string,
            check_if_need_new_version,
        )

        species = clean_string(species)
        species = synonyms(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)

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

        # Loading parser
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

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        # Call appropriate standardisation function with input parameters
        boundary_conditions_data = parser_fn(**parser_input_parameters)

        # Checking against expected format for BoundaryConditions, and align to expected lat/lons if necessary.
        for split_data in boundary_conditions_data.values():

            split_data["data"] = align_lat_lon(data=split_data["data"], domain=domain)

            bc_data = split_data["data"]
            BoundaryConditions.validate_data(bc_data)

        # Check to ensure no required keys are being passed through optional_metadata dict
        self.check_info_keys(optional_metadata)
        if optional_metadata is not None:
            additional_metadata.update(optional_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        boundary_conditions_data = self.update_metadata(
            boundary_conditions_data, additional_input_parameters, additional_metadata
        )

        # This performs the lookup and assignment of data to new or
        # existing Datasources
        data_type = "boundary_conditions"
        datasource_uuids = self.assign_data(
            data=boundary_conditions_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
            filters=filters,
        )

        logger.info(f"Completed processing: {filepath.name}.")

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for boundary conditions Dataset.

        Includes volume mole fractions for each time and ordinal, vertical boundary at the edge of the defined domain:
            - "vmr_n", "vmr_s"
                - expected dimensions: ("time", "height", "lon")
            - "vmr_e", "vmr_w"
                - expected dimensions: ("time", "height", "lat")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for BoundaryConditions.
        """
        from openghg.store import DataSchema

        data_vars: dict[str, tuple[str, ...]] = {
            "vmr_n": ("time", "height", "lon"),
            "vmr_e": ("time", "height", "lat"),
            "vmr_s": ("time", "height", "lon"),
            "vmr_w": ("time", "height", "lat"),
        }
        dtypes = {
            "lat": np.floating,
            "lon": np.floating,
            "height": np.floating,
            "time": np.datetime64,
            "vmr_n": np.floating,
            "vmr_e": np.floating,
            "vmr_s": np.floating,
            "vmr_w": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

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
        data_schema = BoundaryConditions.schema()
        data_schema.validate_data(data)
