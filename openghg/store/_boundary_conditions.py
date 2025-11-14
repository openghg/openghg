from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from openghg.types import pathType, TransformError
from openghg.util import load_transform_parser, check_if_need_new_version, split_function_inputs
import numpy as np

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore

__all__ = ["BoundaryConditions"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _data_type = "boundary_conditions"
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_raw_data(
        self,
        binary_data: bytes,
        metadata: dict,
        file_metadata: dict,
        source_format: str,
    ) -> list[dict] | None:
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

            return self.standardise_and_store(filepath=filepath, source_format=source_format, **metadata)

    def format_inputs(self, **kwargs: Any) -> dict:
        """
        Apply appropriate formatting for expected inputs for BoundaryConditions. Expected
        inputs will typically be defined within the openghg.standardse.standardise_bc()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            dict: Formatted parameters for this data type.

        TODO: Decide if we can phase out additional_metadata or if this could be
            added to params.
        """
        from openghg.util import clean_string, synonyms

        params = kwargs.copy()

        # Apply clean string formatting
        params["species"] = clean_string(params.get("species"))
        params["bc_input"] = clean_string(params.get("bc_input"))
        params["domain"] = clean_string(params.get("domain"))

        # Apply individual formatting as appropriate
        # - apply synonyms substitution for species
        species = params.get("species")
        if species is not None:
            params["species"] = synonyms(species)

        return params

    @staticmethod
    def schema() -> DataSchema:  # type: ignore[override]
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

    def transform_data(
        self,
        datapath: pathType,
        database: str,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        info_metadata: dict | None = None,
        **kwargs: dict,
    ) -> list[dict]:
        """Read and transform a cams boundary conditions data. This will find the appropriate parser function to use for the database specified. The necessary inputs are determined by which database is being used.
        The underlying parser functions will be of the form:
            - openghg.transform.boundary_conditions.parse_{database.lower()}
                - e.g. openghg.transform.boundary_conditions.parse_cams()"""

        from openghg.store.spec import define_transform_parsers

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Format input parameters (specific to data_type)
        fn_input_parameters = self.format_inputs(**kwargs)

        new_version = check_if_need_new_version(if_exists, save_current)

        fn_input_parameters["datapath"] = Path(datapath)

        transform_parsers = define_transform_parsers()[self._data_type]

        try:
            transform_parsers[database.upper()].value
        except KeyError:
            raise ValueError(f"Unable to transform '{database}' selected.")

        # Load the data retrieve object
        parser_fn = load_transform_parser(data_type=self._data_type, source_format=database)

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        # Call appropriate standardisation function with input parameters
        try:
            bc_data = parser_fn(**parser_input_parameters)
        except (TypeError, ValueError) as err:
            msg = f"Error during transformation of data(s): {datapath}. Error: {err}"
            logger.exception(msg)
            raise TransformError(msg)

        # Checking against expected format for Flux
        for mdd in bc_data:
            BoundaryConditions.validate_data(mdd.data)

        required_keys = ("species", "bc_input", "domain")

        if info_metadata:
            common_keys = set(required_keys) & set(info_metadata.keys())

            if common_keys:
                raise ValueError(
                    f"The following optional metadata keys are already present in required keys: {', '.join(common_keys)}"
                )
            else:
                for parsed_data in bc_data:
                    parsed_data.metadata.update(info_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        bc_data = self.update_metadata(
            bc_data, additional_input_parameters, additional_metadata=info_metadata
        )

        datasource_uuids = self.assign_data(
            data=bc_data,
            if_exists=if_exists,
            new_version=new_version,
            required_keys=required_keys,
            compressor=compressor,
            filters=filters,
        )

        return datasource_uuids
