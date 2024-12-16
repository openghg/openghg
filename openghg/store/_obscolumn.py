from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Optional

from numpy import ndarray

# from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import DataArray

ArrayType = Optional[ndarray | DataArray]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObsColumn(BaseStore):
    """This class is used to process emissions / flux data"""

    _data_type = "column"
    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_file(
        self,
        filepath: str | Path,
        species: str,
        platform: str = "satellite",
        satellite: str | None = None,
        domain: str | None = None,
        selection: str | None = None,
        site: str | None = None,
        network: str | None = None,
        instrument: str | None = None,
        source_format: str = "openghg",
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        chunks: dict | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Read column observation file

        Args:
            filepath: Path of observation file
            species: Species name or synonym e.g. "ch4"
            platform: Type of platform. Should be one of:
                - "satellite"
                - "site"
            satellite: Name of satellite (if relevant). Should include satellite OR site.
            domain: For satellite only. If data has been selected on an area include the
                identifier name for domain covered. This can map to previously defined domains
                (see openghg_defs "domain_info.json" file) or a newly defined domain.
            selection: For satellite only, identifier for any data selection which has been
                performed on satellite data. This can be based on any form of filtering, binning etc.
                but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
                If not specified, domain will be used.
            site : Site code/name (if relevant). Should include satellite OR site.
            instrument: Instrument name e.g. "TANSO-FTS"
            network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"
            source_format : Type of data being input e.g. openghg (internal format)
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
            dict: Dictionary of datasource UUIDs data assigned to
        """
        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        from openghg.store.spec import define_standardise_parsers
        from openghg.util import (
            clean_string,
            load_standardise_parser,
            split_function_inputs,
            check_if_need_new_version,
            synonyms,
        )

        # TODO: Evaluate which inputs need cleaning (if any)
        species = clean_string(species)
        species = synonyms(species)
        platform = clean_string(platform)

        if site is None and satellite is None:
            raise ValueError("One of 'site' or 'satellite' must be specified")
        elif site is not None and satellite is not None:
            raise ValueError("Only one of 'site' or 'satellite' should be specified")

        site = clean_string(site)
        satellite = clean_string(satellite)
        domain = clean_string(domain)
        network = clean_string(network)
        instrument = clean_string(instrument)

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

        # Load the data retrieve object
        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)

        # Get current parameter values and filter to only include function inputs
        fn_current_parameters = locals().copy()  # Make a copy of parameters passed to function
        fn_input_parameters = {key: fn_current_parameters[key] for key in fn_input_parameters}

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        obs_data = parser_fn(**parser_input_parameters)

        # TODO: Add in schema and checks for ObsColumn
        # # Checking against expected format for ObsColumn
        # for split_data in obs_data.values():
        #     col_data = split_data["data"]
        #     ObsColumn.validate_data(col_data)

        # TODO: Do we need to do include a split here of some kind, since
        # this could be "site" or "satellite" keys.
        # platform = list(obs_data.keys())[0]["metadata"]["platform"]

        # Check to ensure no required keys are being passed through optional_metadata dict
        self.check_info_keys(optional_metadata)
        if optional_metadata is not None:
            additional_metadata.update(optional_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        obs_data = self.update_metadata(obs_data, additional_input_parameters, additional_metadata)

        data_type = "column"
        datasource_uuids = self.assign_data(
            data=obs_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
            filters=filters,
        )

        # TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
        # update_keys = ["start_date", "end_date", "latest_version"]
        # obs_data = update_metadata(data_dict=obs_data, uuid_dict=datasource_uuids, update_keys=update_keys)

        # obs_store.add_datasources(
        #     uuids=datasource_uuids, data=obs_data, metastore=metastore, update_keys=update_keys
        # )

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    # TODO: Add in transform method for gosat and tropomi raw data files
    #  - Included emissions version as starting point to be updated.
    # @staticmethod
    # def transform_data(
    #     datapath: Union[str, Path],
    #     database: str,
    #     overwrite: bool = False,
    #     **kwargs: Dict,
    # ) -> Dict:
    #     """
    #     Read and transform an emissions database. This will find the appropriate
    #     parser function to use for the database specified. The necessary inputs
    #     are determined by which database ie being used.

    #     The underlying parser functions will be of the form:
    #         - openghg.transform.emissions.parse_{database.lower()}
    #             - e.g. openghg.transform.emissions.parse_edgar()

    #     Args:
    #         datapath: Path to local copy of database archive (for now)
    #         database: Name of database
    #         overwrite: Should this data overwrite currently stored data
    #             which matches.
    #         **kwargs: Inputs for underlying parser function for the database.
    #             Necessary inputs will depend on the database being parsed.

    #     TODO: Could allow Callable[..., Dataset] type for a pre-defined function be passed
    #     """
    #     import inspect
    #     from openghg.store import assign_data, load_metastore, datasource_lookup
    #     from openghg.types import EmissionsDatabases
    #     from openghg.util import load_emissions_database_parser

    #     datapath = Path(datapath)

    #     try:
    #         data_type = EmissionsDatabases[database.upper()].value
    #     except KeyError:
    #         raise ValueError(f"Unable to transform '{database}' selected.")

    #     # Load the data retrieve object
    #     parser_fn = load_emissions_database_parser(database=database)

    #     em_store = Emissions.load()

    #     # Load in the metadata store
    #     metastore = load_metastore(key=em_store._metakey)

    #     # Find all parameters that can be accepted by parse function
    #     all_param = list(inspect.signature(parser_fn).parameters.keys())

    #     # Define parameters to pass to the parser function from kwargs
    #     param: Dict[Any, Any] = {key: value for key, value in kwargs.items() if key in all_param}
    #     param["datapath"] = datapath  # Add datapath explicitly (for now)

    #     emissions_data = parser_fn(**param)

    #     # Checking against expected format for Emissions
    #     for split_data in emissions_data.values():
    #         em_data = split_data["data"]
    #         Emissions.validate_data(em_data)

    #     required = ("species", "source", "domain", "date")
    #     lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

    #     data_type = "emissions"
    #     overwrite = False
    #     datasource_uuids = assign_data(
    #         data_dict=emissions_data,
    #         lookup_results=lookup_results,
    #         overwrite=overwrite,
    #         data_type=data_type,
    #     )

    #     em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore)

    #     em_store.save()
    #     metastore.close()

    #     return datasource_uuids

    # TODO: Define and add schema methods for ObsColumn
    # @staticmethod
    # def schema(species: str, platform: str = "satellite") -> DataSchema:
    #     """
    #     Define schema for emissions Dataset.

    #     Includes column data for each time point:
    #         - standardised species and column name (e.g. "xch4")
    #         - expected dimensions: ("time")

    #     Expected data types for all variables and coordinates also included.

    #     Returns:
    #         DataSchema : Contains schema for Emissions.
    #     """
    #     data_vars: Dict[str, Tuple[str, ...]] \
    #         = {"flux": ("time", "lat", "lon")}
    #     dtypes = {"lat": np.floating,
    #               "lon": np.floating,
    #               "time": np.datetime64,
    #               "flux": np.floating}

    #     data_format = DataSchema(data_vars=data_vars,
    #                              dtypes=dtypes)

    #     return data_format

    # @staticmethod
    # def validate_data(data: Dataset) -> None:
    #     """
    #     Validate input data against Emissions schema - definition from
    #     Emissions.schema() method.

    #     Args:
    #         data : xarray Dataset in expected format

    #     Returns:
    #         None

    #         Raises a ValueError with details if the input data does not adhere
    #         to the Emissions schema.
    #     """
    #     data_schema = Emissions.schema()
    #     data_schema.validate_data(data)
