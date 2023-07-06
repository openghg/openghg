import logging
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Sequence, Tuple, Union

import numpy as np
from pandas import Timedelta
from xarray import Dataset
import inspect
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.types import multiPathType, pathType, resultsType, optionalPathType

__all__ = ["ObsSurface"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObsSurface(BaseStore):
    """This class is used to process surface observation data"""

    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    # @staticmethod
    # def load_metastore() -> TinyDB:
    #     """ Load the ObsSurface metadata store

    #     Returns:
    #         TinyDB: TinyDB object
    #     """

    @staticmethod
    def read_data(
        binary_data: bytes,
        metadata: Dict,
        file_metadata: Dict,
        precision_data: Optional[bytes] = None,
        site_filepath: optionalPathType = None,
    ) -> Dict:
        """Reads binary data passed in by serverless function.
        The data dictionary should contain sub-dictionaries that contain
        data and metadata keys.

        This is clunky and the ObsSurface.read_file function could
        be tidied up quite a lot to be more flexible.

        Args:
            binary_data: Binary measurement data
            metadata: Metadata
            file_metadata: File metadata such as original filename
            precision_data: GCWERKS precision data
            site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
                Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        Returns:
            dict: Dictionary of result
        """
        from tempfile import TemporaryDirectory

        possible_kwargs = {
            "source_format",
            "network",
            "site",
            "inlet",
            "instrument",
            "sampling_period",
            "measurement_type",
            "if_exists",
            "save_current",
            "overwrite",
            "force",
            "source_format",
            "data_type",
        }

        # We've got a lot of functions that expect a file and read
        # metadata from its filename. As Acquire handled all of this behind the scenes
        # we'll create a temporary directory for now
        # TODO - add in just passing a filename to prevent all this read / write
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            meta_kwargs = {k: v for k, v in metadata.items() if k in possible_kwargs}

            if not meta_kwargs:
                raise ValueError("No valid metadata arguments passed, please check documentation.")

            if precision_data is None:
                result = ObsSurface.read_file(filepath=filepath, **meta_kwargs)
            else:
                # We'll assume that if we have precision data it's GCWERKS
                # We don't read anything from the precision filepath so it's name doesn't matter
                precision_filepath = tmpdir_path.joinpath("precision_data.C")
                precision_filepath.write_bytes(precision_data)
                # Create the expected GCWERKS tuple
                result = ObsSurface.read_file(
                    filepath=(filepath, precision_filepath), site_filepath=site_filepath, **meta_kwargs
                )

        return result

    @staticmethod
    def read_file(
        filepath: multiPathType,
        source_format: str,
        network: str,
        site: str,
        inlet: Optional[str] = None,
        height: Optional[str] = None,
        instrument: Optional[str] = None,
        sampling_period: Optional[Union[Timedelta, str]] = None,
        calibration_scale: Optional[str] = None,
        measurement_type: str = "insitu",
        verify_site_code: bool = True,
        site_filepath: optionalPathType = None,
        update_mismatch: str = "never",
        if_exists: Optional[str] = None,
        save_current: Optional[bool] = None,
        overwrite: bool = False,
        force: bool = False,
    ) -> Dict:
        """Process files and store in the object store. This function
            utilises the process functions of the other classes in this submodule
            to handle each data type.

        Args:
            filepath: Filepath(s)
            source_format: Data format, for example CRDS, GCWERKS
            site: Site code/name
            network: Network name
            inlet: Inlet height. Format 'NUMUNIT' e.g. "10m".
                If retrieve multiple files pass None, OpenGHG will attempt to
                extract this from the file.
            height: Alias for inlet.
            read inlets from data.
            instrument: Instrument name
            sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period).
            measurement_type: Type of measurement e.g. insitu, flask
            verify_site_code: Verify the site code
            site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
                Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
                        update_mismatch: This determines whether mismatches between the internal data
                attributes and the supplied / derived metadata can be updated or whether
                this should raise an AttrMismatchError.
                If True, currently updates metadata with attribute value.
            update_mismatch: This determines how mismatches between the internal data
                "attributes" and the supplied / derived "metadata" are handled.
                This includes the options:
                    - "never" - don't update mismatches and raise an AttrMismatchError
                    - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
                    - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
            if_exists: What to do if existing data is present.
                - None - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "replace" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                If None, this will depend on if_exists input (None -> True), (other -> False)
            overwrite: Deprecated. This will use options for if_exists="new" and save_current=True.
            force: Force adding of data even if this is identical to data stored.
        Returns:
            dict: Dictionary of Datasource UUIDs

        TODO: Should "measurement_type" be changed to "platform" to align
        with ModelScenario and ObsColumn?
        """
        import sys
        from collections import defaultdict

        from openghg.store import assign_data, datasource_lookup, load_metastore, update_metadata
        from openghg.types import SurfaceTypes
        from openghg.util import (
            clean_string,
            format_inlet,
            hash_file,
            load_surface_parser,
            verify_site,
            check_if_need_new_version,
        )
        from tqdm import tqdm

        if not isinstance(filepath, list):
            filepath = [filepath]

        try:
            source_format = SurfaceTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Test that the passed values are valid
        # Check validity of site, instrument, inlet etc in 'site_info.json'
        # Clean the strings
        if verify_site_code:
            verified_site = verify_site(site=site)
            if verified_site is None:
                raise ValueError("Unable to validate site")
            else:
                site = verified_site
        else:
            site = clean_string(site)

        network = clean_string(network)
        instrument = clean_string(instrument)

        # Check if alias `height` is included instead of `inlet`
        if inlet is None and height is not None:
            inlet = height

        # Try to ensure inlet is 'NUM''UNIT' e.g. "10m"
        inlet = clean_string(inlet)
        inlet = format_inlet(inlet)

        if overwrite and if_exists is None:
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure data can be force overwritten if force keyword is included.
        if force and if_exists is None:
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        sampling_period_seconds: Union[str, None] = None
        # If we have a sampling period passed we want the number of seconds
        if sampling_period is not None:
            # Check value passed is not just a number with no units
            try:
                float(sampling_period)
            except (ValueError, TypeError):
                # If this cannot be evaluated to a float assume this is correct form.
                pass
            else:
                raise ValueError(
                    f"Invalid sampling period: '{sampling_period}'. Must be specified as a string with unit (e.g. 1m for 1 minute)."
                )

            # Check string passed can be evaluated as a Timedelta object
            # and extract this in seconds.
            try:
                sampling_period_td = Timedelta(sampling_period)
            except ValueError:
                raise ValueError(
                    f"Could not evaluate sampling period: '{sampling_period}'. Must be specified as a string with valid unit (e.g. 1m for 1 minute)."
                )

            sampling_period_seconds = str(float(sampling_period_td.total_seconds()))

            # Check if sampling period has resolved to 0 seconds.
            if sampling_period_seconds == "0.0":
                raise ValueError(
                    f"Sampling period resolves to <= 0.0 seconds. Please check input: '{sampling_period}'"
                )

            # TODO: May want to add check for NaT or NaN

        # Load the data retrieve object
        parser_fn = load_surface_parser(source_format=source_format)

        obs = ObsSurface.load()

        results: resultsType = defaultdict(dict)

        # Load the store for the metadata
        metastore = load_metastore(key=obs._metakey)

        # Create a progress bar object using the filepaths, iterate over this below
        with tqdm(total=len(filepath), file=sys.stdout) as progress_bar:
            for fp in filepath:
                if source_format == "GCWERKS":
                    if not isinstance(fp, tuple):
                        raise TypeError("For GCWERKS data we expect a tuple of (data file, precision file).")

                    try:
                        data_filepath = Path(fp[0])
                        precision_filepath = Path(fp[1])
                    except (ValueError, TypeError):
                        raise TypeError(
                            "For GCWERKS data both data and precision filepaths must be given as a tuple."
                        )
                else:
                    data_filepath = Path(fp)

                file_hash = hash_file(filepath=data_filepath)
                if file_hash in obs._file_hashes and not force:
                    logger.warning(
                        "This file has been uploaded previously with the filename : "
                        f"{obs._file_hashes[file_hash]} - skipping.\n"
                        "If necessary, use force=True to bypass this to add this data."
                    )
                    break

                # Define required input parameters for parser function
                required_parameters = {
                    "data_filepath": data_filepath,
                    "site": site,
                    "network": network,
                    "inlet": inlet,
                    "instrument": instrument,
                    "sampling_period": sampling_period_seconds,
                    "measurement_type": measurement_type,
                    "site_filepath": site_filepath,
                }
                if source_format == "GCWERKS":
                    required_parameters["precision_filepath"] = precision_filepath

                # Collect together optional parameters (not required but
                # may be accepted by underlying parser function)
                optional_parameters = {"update_mismatch": update_mismatch}
                # TODO: extend optional_parameters to include kwargs when added

                input_parameters = required_parameters.copy()

                # Find parameters that parser_fn accepts (must accept all required arguments already)
                signature = inspect.signature(parser_fn)
                fn_accepted_parameters = [param.name for param in signature.parameters.values()]

                # Check if optional parameters are present in function call and only use those which are.
                for param, param_value in optional_parameters.items():
                    if param in fn_accepted_parameters:
                        input_parameters[param] = param_value
                    else:
                        logger.warning(
                            f"Input: '{param}' (value: {param_value}) is not being used as part of the standardisation process."
                            f"This is not accepted by the current standardisation function: {parser_fn}"
                        )

                progress_bar.set_description(f"Processing: {data_filepath.name}")

                # Call appropriate standardisation function with input parameters
                data = parser_fn(**input_parameters)

                # Current workflow: if any species fails, whole filepath fails
                for key, value in data.items():
                    species = key.split("_")[0]
                    try:
                        ObsSurface.validate_data(value["data"], species=species)
                    except ValueError:
                        logger.error(
                            f"Unable to validate and store data from file: {data_filepath.name}.",
                            f" Problem with species: {species}\n",
                        )
                        validated = False
                        break
                else:
                    validated = True

                if not validated:
                    continue

                # Alternative workflow: Would only stops certain species within a
                # file being written to the object store.
                # to_remove = []
                # for key, value in data.items():
                #     species = key.split('_')[0]
                #     try:
                #         ObsSurface.validate_data(value["data"], species=species)
                #     except ValueError:
                #         print(f"WARNING: standardised data for '{source_format}' is not in expected OpenGHG format.")
                #         print(f"Check data for {species}")
                #         print(value["data"])
                #         print("Not writing to object store.")
                #         to_remove.append(key)
                #
                # for remove in to_remove:
                #     data.pop(remove)

                required_keys = (
                    "species",
                    "site",
                    "sampling_period",
                    "station_long_name",
                    "inlet",
                    "instrument",
                    "network",
                    "source_format",
                    "data_source",
                    "icos_data_level",
                    "data_type",
                )

                lookup_results = datasource_lookup(
                    metastore=metastore, data=data, required_keys=required_keys, min_keys=5
                )

                # Create Datasources, save them to the object store and get their UUIDs
                data_type = "surface"
                datasource_uuids = assign_data(
                    data_dict=data,
                    lookup_results=lookup_results,
                    if_exists=if_exists,
                    new_version=new_version,
                    data_type=data_type,
                )

                update_keys = ["start_date", "end_date", "latest_version"]
                data = update_metadata(data_dict=data, uuid_dict=datasource_uuids, update_keys=update_keys)

                results["processed"][data_filepath.name] = datasource_uuids

                # Record the Datasources we've created / appended to
                obs.add_datasources(
                    uuids=datasource_uuids, data=data, metastore=metastore, update_keys=update_keys
                )

                # Store the hash as the key for easy searching, store the filename as well for
                # ease of checking by user
                obs._file_hashes[file_hash] = data_filepath.name
                # except Exception:
                #     results["error"][data_filepath.name] = traceback.format_exc()

                progress_bar.update(1)

                logger.info(f"Completed processing: {data_filepath.name}.")
                # logger.info(f"\tUUIDs: {datasource_uuids}")

        # Ensure we explicitly close the metadata store
        # as we're using the cached storage method
        metastore.close()
        # Save this object back to the object store
        obs.save()

        return dict(results)

    @staticmethod
    def read_multisite_aqmesh(
        data_filepath: pathType,
        metadata_filepath: pathType,
        network: str = "aqmesh_glasgow",
        instrument: str = "aqmesh",
        sampling_period: int = 60,
        measurement_type: str = "insitu",
        if_exists: Optional[str] = None,
        overwrite: bool = False,
    ) -> DefaultDict:
        """Read AQMesh data for the Glasgow network

        NOTE - temporary function until we know what kind of AQMesh data
        we'll be retrieve in the future.

        This data is different in that it contains multiple sites in the same file.
        """
        from collections import defaultdict

        from openghg.standardise.surface import parse_aqmesh
        from openghg.store import assign_data, datasource_lookup, load_metastore
        from openghg.util import hash_file
        from tqdm import tqdm

        data_filepath = Path(data_filepath)
        metadata_filepath = Path(metadata_filepath)

        if overwrite and if_exists is None:
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` input."
                "See documentation for details of this input and options."
            )
            if_exists = "new"

        # Load the ObsSurface object for retrieve
        obs = ObsSurface.load()
        # Load the metadata store
        metastore = load_metastore(key=obs._metakey)
        # Get a dict of data and metadata
        processed_data = parse_aqmesh(data_filepath=data_filepath, metadata_filepath=metadata_filepath)

        results: resultsType = defaultdict(dict)
        for site, site_data in tqdm(processed_data.items()):
            metadata = site_data["metadata"]
            measurement_data = site_data["data"]

            file_hash = hash_file(filepath=data_filepath)

            if obs.seen_hash(file_hash=file_hash) and if_exists is None:
                raise ValueError(
                    f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]}."
                )
                break

            combined = {site: {"data": measurement_data, "metadata": metadata}}

            required_keys = (
                "site",
                "species",
                "inlet",
                "network",
                "instrument",
                "sampling_period",
                "measurement_type",
            )

            lookup_results = datasource_lookup(
                metastore=metastore, data=combined, required_keys=required_keys, min_keys=5
            )

            uuid = lookup_results[site]

            # Jump through these hoops until we can rework the data assignment functionality to split it out
            # into more sensible functions
            # TODO - fix the assign data function to avoid this kind of hoop jumping
            lookup_result = {site: uuid}

            # Create Datasources, save them to the object store and get their UUIDs
            data_type = "surface"
            datasource_uuids = assign_data(
                data_dict=combined,
                lookup_results=lookup_result,
                if_exists=if_exists,
                data_type=data_type,
            )

            results[site] = datasource_uuids

            # Record the Datasources we've created / appended to
            obs.add_datasources(uuids=datasource_uuids, data=combined, metastore=metastore)

            # Store the hash as the key for easy searching, store the filename as well for
            # ease of checking by user
            obs.set_hash(file_hash=file_hash, filename=data_filepath.name)

        obs.save()
        # Close the metadata store and write new records
        metastore.close()

        return results

    @staticmethod
    def schema(species: str) -> DataSchema:
        """
        Define schema for surface observations Dataset.

        Only includes mandatory variables
            - standardised species name (e.g. "ch4")
            - expected dimensions: ("time")

        Expected data types for variables and coordinates also included.

        Returns:
            DataSchema : Contains basic schema for ObsSurface.

        # TODO: Decide how to best incorporate optional variables
        # e.g. "ch4_variability", "ch4_number_of_observations"
        """
        from openghg.standardise.meta import define_species_label

        name = define_species_label(species)[0]

        data_vars: Dict[str, Tuple[str, ...]] = {name: ("time",)}
        dtypes = {name: np.floating, "time": np.datetime64}

        source_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return source_format

    @staticmethod
    def validate_data(data: Dataset, species: str) -> None:
        """
        Validate input data against ObsSurface schema - definition from
        ObsSurface.schema() method.

        Args:
            data : xarray Dataset in expected format
            species: Species name

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the ObsSurface schema.
        """
        data_schema = ObsSurface.schema(species)
        data_schema.validate_data(data)

    @staticmethod
    def store_data(
        data: Dict,
        if_exists: Optional[str] = None,
        overwrite: bool = False,
        required_metakeys: Optional[Sequence] = None,
    ) -> Optional[Dict]:
        """This expects already standardised data such as ICOS / CEDA

        Args:
            data: Dictionary of data in standard format, see the data spec under
            Development -> Data specifications in the documentation
            if_exists: What to do if existing data is present.
                - None - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "replace" - replace and insert new data into current timeseries
            overwrite: Deprecated. This will use options for if_exists="new".            required_metakeys: Keys in the metadata we should use to store this metadata in the object store
            if None it defaults to:
            {"species", "site", "station_long_name", "inlet", "instrument",
            "network", "source_format", "data_source", "icos_data_level"}
        Returns:
            Dict or None:
        """
        from openghg.store import assign_data, datasource_lookup, load_metastore
        from openghg.util import hash_retrieved_data

        if overwrite and if_exists is None:
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` input."
                "See documentation for details of this input and options."
            )
            if_exists = "new"

        obs = ObsSurface.load()
        metastore = load_metastore(key=obs._metakey)

        # Very rudimentary hash of the data and associated metadata
        hashes = hash_retrieved_data(to_hash=data)
        # Find the keys in data we've seen before
        seen_before = {next(iter(v)) for k, v in hashes.items() if k in obs._retrieved_hashes}

        if len(seen_before) == len(data):
            logger.warning("Note: There is no new data to process.")
            return None

        keys_to_process = set(data.keys())
        if seen_before:
            # TODO - add this to log
            logger.warning(f"Note: We've seen {seen_before} before. Processing new data only.")
            keys_to_process -= seen_before

        to_process = {k: v for k, v in data.items() if k in keys_to_process}

        if required_metakeys is None:
            required_metakeys = (
                "species",
                "site",
                "station_long_name",
                "inlet",
                "instrument",
                "network",
                "source_format",
                "data_source",
                "icos_data_level",
            )
            min_keys = 5
        else:
            min_keys = len(required_metakeys)

        lookup_results = datasource_lookup(
            metastore=metastore, data=to_process, required_keys=required_metakeys, min_keys=min_keys
        )

        # Create Datasources, save them to the object store and get their UUIDs
        data_type = "surface"
        datasource_uuids = assign_data(
            data_dict=to_process,
            lookup_results=lookup_results,
            if_exists=if_exists,
            data_type=data_type,
        )

        # Record the Datasources we've created / appended to
        obs.add_datasources(uuids=datasource_uuids, data=to_process, metastore=metastore)
        obs.store_hashes(hashes=hashes)

        metastore.close()
        obs.save()

        return datasource_uuids

    def store_hashes(self, hashes: Dict) -> None:
        """Store hashes of data retrieved from a remote data source such as
        ICOS or CEDA. This takes the full dictionary of hashes, removes the ones we've
        seen before and adds the new.

        Args:
            hashes: Dictionary of hashes provided by the hash_retrieved_data function
        Returns:
            None
        """
        new = {k: v for k, v in hashes.items() if k not in self._retrieved_hashes}
        self._retrieved_hashes.update(new)

    def delete(self, uuid: str) -> None:
        """Delete a Datasource with the given UUID

        This function deletes both the record of the object store in he

        Args:
            uuid (str): UUID of Datasource
        Returns:
            None
        """
        from openghg.objectstore import delete_object, get_bucket
        from openghg.store import load_metastore
        from openghg.store.base import Datasource
        from tinydb import where

        bucket = get_bucket()
        # Load the Datasource and get all its keys
        # iterate over these keys and delete them
        datasource = Datasource.load(uuid=uuid)

        data_keys = datasource.raw_keys()

        for version in data_keys:
            key_data = data_keys[version]["keys"]

            for daterange in key_data:
                key = key_data[daterange]
                delete_object(bucket=bucket, key=key)

        # Then delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=bucket, key=key)

        # Delete the UUID from the metastore
        metastore = load_metastore(key=self._metakey)
        metastore.remove(where("uuid") == uuid)
        metastore.close

        del self._datasource_uuids[uuid]

    def seen_hash(self, file_hash: str) -> bool:
        return file_hash in self._file_hashes

    def set_hash(self, file_hash: str, filename: str) -> None:
        self._file_hashes[file_hash] = filename
