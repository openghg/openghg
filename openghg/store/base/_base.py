""" This file contains the BaseStore class from which other storage
    modules inherit.
"""

from __future__ import annotations
import logging
import math
from pathlib import Path
from pandas import Timestamp
from types import TracebackType
from typing import Any, TypeVar
from collections.abc import Sequence
import xarray as xr

from openghg.objectstore import get_object_from_json, exists, set_object_from_json, get_metakeys
from openghg.objectstore.metastore import DataClassMetaStore
from openghg.store.storage import ChunkingSchema
from openghg.types import DatasourceLookupError, multiPathType
from openghg.util import timestamp_now, to_lowercase, hash_file


T = TypeVar("T", bound="BaseStore")

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BaseStore:
    _registry: dict[str, type[BaseStore]] = {}
    _data_type = ""
    _root = "root"
    _uuid = "root_uuid"

    def __init__(self, bucket: str) -> None:
        # from openghg.objectstore import get_object_from_json, exists

        self._creation_datetime = str(timestamp_now())
        self._stored = False
        # Hashes of previously uploaded files
        self._file_hashes: dict[str, str] = {}
        # Hashes of previously stored data from other data platforms
        self._retrieved_hashes: dict[str, dict] = {}
        # Where we'll store this object's metastore
        self._metakey = ""

        if exists(bucket=bucket, key=self.key()):
            data = get_object_from_json(bucket=bucket, key=self.key())
            # Update myself
            self.__dict__.update(data)

        self._metastore = DataClassMetaStore(bucket=bucket, data_type=self._data_type)
        self._bucket = bucket
        self._datasource_uuids = self._metastore.select("uuid")

    def __init_subclass__(cls) -> None:
        BaseStore._registry[cls._data_type] = cls

    def __enter__(self) -> BaseStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            logger.error(msg="", exc_info=exc_val)
        else:
            self.save()

    @classmethod
    def metakey(cls) -> str:
        return str(cls._metakey)

    @classmethod
    def key(cls) -> str:
        return f"{cls._root}/uuid/{cls._uuid}"

    def save(self) -> None:
        # from openghg.objectstore import set_object_from_json

        self._metastore.close()
        set_object_from_json(bucket=self._bucket, key=self.key(), data=self.to_data())

    def to_data(self) -> dict:
        # We don't need to store the metadata store, it has its own location
        # QUESTION - Is this cleaner than the previous specifying
        DO_NOT_STORE = ["_metastore", "_bucket", "_datasource_uuids"]
        return {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}

    def read_data(self, *args: Any, **kwargs: Any) -> dict | None:
        raise NotImplementedError

    def read_file(self, *args: Any, **kwargs: Any) -> dict:
        raise NotImplementedError

    def store_data(self, *args: Any, **kwargs: Any) -> dict | None:
        raise NotImplementedError

    def transform_data(self, *args: Any, **kwargs: Any) -> dict:
        raise NotImplementedError

    def chunking_schema(self) -> ChunkingSchema:
        raise NotImplementedError

    def store_hashes(self, hashes: dict[str, Path]) -> None:
        """Store the hashes of files we've seen before

        Args:
            hahes: Dictionary of hashes
        Returns:
            None
        """
        name_only = {k: v.name for k, v in hashes.items()}
        self._file_hashes.update(name_only)

    def check_hashes(self, filepaths: multiPathType, force: bool) -> tuple[dict[str, Path], dict[str, Path]]:
        """Check the hashes of the files passed against the hashes of previously
        uploaded files. Two dictionaries are returned, one containing the hashes
        of files we've seen before and one containing the hashes of files we haven't.

        A warning is logged if we've seen any of the files before

        Args:
            filepaths: List of filepaths
            force: If force is True then we will expect to process all the filepaths, not just the
            unseen ones
        Returns:
            tuple: seen files, unseen files
        """
        if not isinstance(filepaths, list):
            filepaths = [filepaths]

        filepaths = [Path(filepath) for filepath in filepaths]

        unseen: dict[str, Path] = {}
        seen: dict[str, Path] = {}

        for filepath in filepaths:
            file_hash = hash_file(filepath=filepath)
            if file_hash in self._file_hashes:
                seen[file_hash] = filepath
            else:
                unseen[file_hash] = filepath

        if force:
            unseen = {**seen, **unseen}

        if seen:
            logger.warning("Skipping previously standardised files, see log for list.")
            seen_files_msg = "\n".join([str(v) for v in seen.values()])
            logger.debug(f"We've seen the following files before:\n{seen_files_msg}")

            if unseen:
                logger.info(f"Processing {len(unseen)} files of {len(filepaths)}.")

        if unseen:
            to_process = "\n".join([str(v) for v in unseen.values()])
            logger.debug(f"Processing the following files:\n{to_process}")
        else:
            logger.info("No new files to process.")

        return seen, unseen

    def add_metakeys(self, force: bool = False) -> dict:
        """
        Check metakeys are included from relevant config file and add as the `.metakeys`
        attributes if not.
        """
        if not hasattr(self, "metakeys") or force:
            try:
                metakeys = get_metakeys(bucket=self._bucket)[self._data_type]
            except KeyError:
                raise ValueError(
                    f"No metakeys for {self._data_type}, please update metakeys configuration file."
                )

            self.metakeys = metakeys

        return self.metakeys

    def update_metadata(self, data: dict, input_parameters: dict, additional_metadata: dict) -> dict:
        """This adds additional metadata keys to the metadata within the data dictionary.

        Args:
            data: Dictionary containing data and metadata for datasource
            input_parameters: Input parameters from read_file...
            additional_metadata: Keys to add to the metadata dictionary
        Returns:
            dict: data dictionary with metadata keys added
        """
        from openghg.util import merge_dict

        # Get defined metakeys from the config setup
        metakeys = self.add_metakeys()
        required = metakeys["required"]
        # We might not get any optional keys
        optional = metakeys.get("optional", {})

        for parsed_data in data.values():
            metadata = parsed_data["metadata"]

            # Sources of additional metadata - order in list is order of preference.
            sources = [input_parameters, additional_metadata]
            for source in sources:
                # merge "required" keys from source into metadata; on conflict, keep value from metadata
                metadata = merge_dict(metadata, source, keys_right=required)

            required_not_found = set(required) - set(metadata.keys())
            if required_not_found:
                raise ValueError(
                    f"The following required keys are missing: {', '.join(required_not_found)}. Please specify."
                )

            # Check if named optional keys are included in the input_parameters and add
            optional_matched = set(optional) & set(input_parameters.keys())
            metadata = merge_dict(metadata, input_parameters, keys_right=optional_matched)

            # Add additional metadata keys
            if additional_metadata:
                # Ensure required keys aren't added again (or clash with values from input_parameters)
                additional_metadata_to_add = set(additional_metadata.keys()) - set(required)
                metadata = merge_dict(metadata, additional_metadata, keys_right=additional_metadata_to_add)

            parsed_data["metadata"] = metadata

        return data

    def check_info_keys(self, optional_metadata: dict | None) -> None:
        """Check the informational metadata is not being used to set required keys.

        Args:
            optional_metadata: Additional informational metadata
        Returns:
            None
        Raises:
            ValueError: if any keys within optional_metadata are within the required set of keys.
        """
        metakeys = self.add_metakeys()
        required = metakeys["required"]

        # Check if anything in optional_metadata tries to override our required keys
        if optional_metadata is not None:
            common_keys = set(required) & set(optional_metadata.keys())

            if common_keys:
                raise ValueError(
                    f"The following optional metadata keys are already present in required keys: {', '.join(common_keys)}"
                )

    def get_lookup_keys(self, data: dict) -> list[str]:
        """This creates the list of keys required to perform the Datasource lookup.
        If optional_metadata is passed in then those keys may be taken into account
        if they exist in the list of stored optional keys.

        Args:
            data: Dictionary containing data and metadata for datasource

        Returns:
            tuple: Tuple of keys
        """
        metakeys = self.add_metakeys()
        required = metakeys["required"]
        # We might not get any optional keys
        optional = metakeys.get("optional", {})

        lookup_keys = list(required)

        # Note: Just grabbing the first entry in data at the moment
        # In principle the metadata should have the same keys for all entries
        # but should check that assumption is reasonable
        parsed_data_representative = list(data.values())[0]
        metadata = parsed_data_representative["metadata"]

        # Matching between potential optional keys and those present in the metadata
        optional_lookup = set(optional) & set(metadata.keys())
        lookup_keys.extend(list(optional_lookup))

        return lookup_keys

    def assign_data(
        self,
        data: dict,
        data_type: str,
        required_keys: Sequence[str] | None = None,
        sort: bool = True,
        drop_duplicates: bool = True,
        min_keys: int | None = None,
        update_keys: list | None = None,
        if_exists: str = "auto",
        new_version: bool = True,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> dict[str, dict]:
        """Assign data to a Datasource. This will either create a new Datasource
        Create or get an existing Datasource for each gas in the file

            Args:
                data: Dictionary containing data and metadata for species
                overwrite: If True overwrite current data stored
                data_type: Type of data, timeseries etc
                required_keys: Required minimum keys to lookup unique Datasource
                sort: Sort data in time dimension
                drop_duplicates: Drop duplicate timestamps, keeping the first value
                min_keys: Minimum number of metadata keys needed to uniquely match a Datasource
                if_exists: What to do if existing data is present.
                    - "auto" - checks new and current data for timeseries overlap
                        - adds data if no overlap
                        - raises DataOverlapError if there is an overlap
                    - "new" - just include new data and ignore previous
                    - "combine" - replace and insert new data into current timeseries
                new_version: Create a new version for the data and save current
                    data to a previous version.
                compressor: Compression for zarr encoding
                filters: Filters for zarr encoding
            Returns:
                dict: Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
        """
        from openghg.store.base import Datasource
        from openghg.util import not_set_metadata_values

        uuids = {}

        # Get the metadata keys for this type
        # metakeys = self.get_metakeys()

        if not required_keys:
            required_keys = self.get_lookup_keys(data=data)

        self._metastore.acquire_lock()
        try:
            lookup_results = self.datasource_lookup(data=data, required_keys=required_keys, min_keys=min_keys)
            # TODO - remove this when the lowercasing of metadata gets removed
            # We currently lowercase all the metadata and some keys we don't want to change, such as paths to the object store
            skip_keys = ["object_store"]

            for key, parsed_data in data.items():
                metadata = parsed_data["metadata"]
                dataset = parsed_data["data"]

                # Our lookup results and gas data have the same keys
                uuid = lookup_results[key]

                ignore_values = not_set_metadata_values()

                # Do we want all the metadata in the Dataset attributes?
                to_add = {
                    k: v for k, v in metadata.items() if k not in dataset.attrs and v not in ignore_values
                }
                dataset.attrs.update(to_add)

                # Take a copy of the metadata so we can update it
                meta_copy = metadata.copy()
                new_ds = uuid is False

                if new_ds:
                    datasource = Datasource(bucket=self._bucket)
                    uid = datasource.uuid()
                    meta_copy["uuid"] = uid
                    # Make sure all the metadata is lowercase for easier searching later
                    # TODO - do we want to do this or should be just perform lowercase comparisons?
                    meta_copy = to_lowercase(d=meta_copy, skip_keys=skip_keys)
                else:
                    datasource = Datasource(bucket=self._bucket, uuid=uuid)

                # Add the dataframe to the datasource
                datasource.add_data(
                    metadata=meta_copy,
                    data=dataset,
                    sort=sort,
                    drop_duplicates=drop_duplicates,
                    skip_keys=skip_keys,
                    new_version=new_version,
                    if_exists=if_exists,
                    data_type=data_type,
                    compressor=compressor,
                    filters=filters,
                )

                # Save Datasource to object store
                datasource.save()

                # Add the metadata to the metastore and make sure it's up to date with the metadata stored
                # in the Datasource
                datasource_metadata = datasource.metadata()

                if new_ds:
                    self._metastore.insert(datasource_metadata)
                else:
                    self._metastore.update(where={"uuid": datasource.uuid()}, to_update=datasource_metadata)

                uuids[key] = {"uuid": datasource.uuid(), "new": new_ds}
        finally:
            self._metastore.release_lock()

        return uuids

    def datasource_lookup(
        self, data: dict, required_keys: Sequence[str], min_keys: int | None = None
    ) -> dict:
        """Search the metadata store for a Datasource UUID using the metadata in data. We expect the required_keys
        to be present and will require at least min_keys of these to be present when searching.

        As some metadata value might change (such as data owners etc) we don't want to do an exact
        search on *all* the metadata so we extract a subset (the required keys) and search for these.

        Args:
            metastore: Metadata database
            data: Combined data dictionary of form {key: {data: Dataset, metadata: Dict}}
            required_keys: Iterable of keys to extract from metadata
            min_keys: The minimum number of required keys, if not given it will be set
            to the length of required_keys
        Return:
            dict: Dictionary of datasource information
        """
        from openghg.util import to_lowercase

        if min_keys is None:
            min_keys = len(required_keys)

        results = {}
        for key, _data in data.items():
            metadata = _data["metadata"]

            required_metadata = {
                k.lower(): to_lowercase(v) for k, v in metadata.items() if k in required_keys
            }

            if len(required_metadata) < min_keys:
                missing_keys = set(required_keys) - set(required_metadata)
                raise ValueError(
                    f"The given metadata doesn't contain enough information, we need: {required_keys}\n"
                    + f"Missing keys: {missing_keys}"
                )

            required_result = self._metastore.search(required_metadata)

            if not required_result:
                results[key] = False
            elif len(required_result) > 1:
                raise DatasourceLookupError("More than one Datasource found for metadata, refine lookup.")
            else:
                results[key] = required_result[0]["uuid"]

        return results

    def uuid(self) -> str:
        """Return the UUID of this object

        Returns:
            str: UUID of object
        """
        return self._uuid

    def datasources(self) -> list[str]:
        """Return the list of Datasources UUIDs associated with this object

        Returns:
            list: List of Datasource UUIDs
        """
        return self._datasource_uuids

    def get_rank(self, uuid: str, start_date: Timestamp, end_date: Timestamp) -> dict:
        """Get the rank for the given Datasource for a given date range

        Args:
            uuid: UUID of Datasource
            start_date: Start date
            end_date: End date
        Returns:
            dict: Dictionary of rank and daterange covered by that rank
        """
        raise NotImplementedError("Ranking is being reworked and will be reactivated in a future release.")
        from collections import defaultdict
        from openghg.util import create_daterange_str, daterange_overlap

        if uuid not in self._rank_data:
            return {}

        search_daterange = create_daterange_str(start=start_date, end=end_date)

        rank_data = self._rank_data[uuid]

        ranked = defaultdict(list)
        # Check if this Datasource is ranked for the dates passed
        for daterange, rank in rank_data.items():
            if daterange_overlap(daterange_a=search_daterange, daterange_b=daterange):
                ranked[rank].append(daterange)

        return ranked

    def clear_rank(self, uuid: str) -> None:
        """Clear the ranking data for a Datasource

        Args:
            uuid: UUID of Datasource
        Returns:
            None
        """
        raise NotImplementedError("Ranking is being reworked and will be reactivated in a future release.")
        if uuid in self._rank_data:
            del self._rank_data[uuid]
            self.save()
        else:
            raise ValueError("No ranking data set for that UUID.")

    def set_rank(
        self,
        uuid: str,
        rank: int | str,
        date_range: str | list[str],
        overwrite: bool | None = False,
    ) -> None:
        """Set the rank of a Datasource associated with this object.

        This function performs checks to ensure multiple ranks aren't set for
        overlapping dateranges.

        Passing a daterange and rank to this function will overwrite any current
        daterange stored for that rank.

        Args:
            uuid: UUID of Datasource
            rank: Rank of data
            date_range: Daterange(s)
            overwrite: Overwrite current ranking data
        Returns:
            None
        """
        raise NotImplementedError("Ranking is being reworked and will be reactivated in a future release.")
        from copy import deepcopy

        from openghg.util import (
            combine_dateranges,
            daterange_contains,
            daterange_overlap,
            sanitise_daterange,
            split_encompassed_daterange,
            trim_daterange,
        )

        rank = int(rank)

        if not 1 <= rank <= 10:
            raise TypeError("Rank can only take values 1 to 10 (for unranked). Where 1 is the highest rank.")

        if not isinstance(date_range, list):
            date_range = [date_range]

        # Make sure the dateranges passed are correct and are tz-aware
        date_range = [sanitise_daterange(d) for d in date_range]
        # Combine in case we have overlappng dateranges
        date_range = combine_dateranges(date_range)

        # Used to store dateranges that need to be trimmed to ensure no daterange overlap
        to_update = []
        # Non-overlapping dateranges that can be stored directly
        to_add = []

        if uuid in self._rank_data:
            rank_data = self._rank_data[uuid]
            # Check this source isn't ranked differently for the same dates
            for new_daterange in date_range:
                overlap = False
                # Check for overlapping dateranges and add
                for existing_daterange, existing_rank in rank_data.items():
                    if daterange_overlap(daterange_a=new_daterange, daterange_b=existing_daterange):
                        overlap = True

                        if rank != existing_rank and overwrite:
                            # Save the daterange we need to update
                            to_update.append((existing_daterange, new_daterange))
                            continue
                        # If the ranks are the same we just want to combine the dateranges
                        elif rank == existing_rank:
                            to_combine = [new_daterange, existing_daterange]
                            combined = combine_dateranges(dateranges=to_combine)[0]
                            to_update.append((existing_daterange, combined))
                        else:
                            raise ValueError(
                                f"This datasource has rank {existing_rank} for dates that overlap the ones given. \
                                                Overlapping dateranges are {new_daterange} and {existing_daterange}"
                            )
                # Otherwise we just want to add the new daterange to the dict
                if not overlap:
                    to_add.append(new_daterange)

            # If we've got dateranges to update and ranks to overwrite we need to trim the
            # previous ranking daterange down so we don't have overlapping dateranges
            if to_update:
                # Here we first take a backup of the old ranking data, update
                # it and then write it back
                ranking_backup = deepcopy(rank_data)

                for existing, new in to_update:
                    # Remove the existing daterange key
                    # Here we pass if it doesn't exist as if we have multiple new dateranges
                    # that overlap the existing daterange it might have been deleted during
                    # a previous iteration
                    try:
                        del ranking_backup[existing]
                    except KeyError:
                        pass

                    # If we want to overwrite an existing rank we need to trim that daterange and
                    # rewrite it back to the dictionary
                    rank_copy = rank_data[existing]

                    if overwrite:
                        if existing == new:
                            ranking_backup[new] = rank_copy
                        # If the existing daterange contains the new daterange
                        # we need to split it into parts and save those
                        elif daterange_contains(container=existing, contained=new):
                            result = split_encompassed_daterange(container=existing, contained=new)

                            existing_start = result["container_start"]
                            ranking_backup[existing_start] = rank_copy

                            updated_new = result["contained"]
                            ranking_backup[updated_new] = rank

                            # We might only end up with two dateranges
                            try:
                                existing_end = result["container_end"]
                                ranking_backup[existing_end] = rank_copy
                            except KeyError:
                                pass
                        # If the new daterange contains the existing we can just overwrite it
                        elif daterange_contains(container=new, contained=existing):
                            ranking_backup[new] = rank
                        else:
                            trimmed = trim_daterange(to_trim=existing, overlapping=new)
                            ranking_backup[trimmed] = rank_copy
                            ranking_backup[new] = rank
                    elif rank_copy == rank:
                        # If we're not overwriting we just need to update to use the new combined
                        ranking_backup[new] = rank_copy

                self._rank_data[uuid] = ranking_backup

            # Finally, store the dateranges that didn't overlap
            for d in to_add:
                self._rank_data[uuid][d] = rank
        else:
            for d in date_range:
                self._rank_data[uuid][d] = rank

        self.save()

    def rank_data(self) -> dict:
        """Return a dictionary of rank data keyed
        by UUID

            Returns:
                dict: Dictionary of rank data
        """
        raise NotImplementedError("Ranking is being reworked and will be reactivated in a future release.")
        rank_dict: dict = self._rank_data.to_dict()
        return rank_dict

    def clear_datasources(self) -> None:
        """Remove all Datasources from the object

        Returns:
            None
        """
        self._datasource_uuids.clear()
        self._file_hashes.clear()

    def check_chunks(
        self,
        ds: xr.Dataset,
        chunks: dict[str, int] | None = None,
        max_chunk_size: int = 300,
        **chunking_kwargs: Any,
    ) -> dict[str, int]:
        """Check the chunk size of a variable in a dataset and return the chunk size

        Args:
            ds: dataset to check
            variable: Name of the variable that we want to check for max chunksize
            chunk_dimension: Dimension to chunk over
            secondary_dimensions: List of secondary dimensions to chunk over
            max_chunk_size: Maximum chunk size in megabytes, defaults to 300 MB
        Returns:
            Dict: Dictionary of chunk sizes
        """
        try:
            default_schema = self.chunking_schema(**chunking_kwargs)
        except NotImplementedError:
            logger.warn(f"No chunking schema found for {type(self).__name__}")
            return {}

        variable = default_schema.variable
        default_chunks = default_schema.chunks
        secondary_dimensions = default_schema.secondary_dims

        dim_sizes = dict(ds[variable].sizes)
        var_dtype_bytes = ds[variable].dtype.itemsize

        if secondary_dimensions is not None:
            missing_dims = [dim for dim in secondary_dimensions if dim not in dim_sizes]
            if missing_dims:
                raise ValueError(f"The following dimensions are missing: {missing_dims}")

        # Make the 'chunks' dict, using dim_sizes for any unspecified dims
        specified_chunks = default_chunks if chunks is None else chunks
        # TODO - revisit this type hinting
        chunks = dict(dim_sizes, **specified_chunks)  # type: ignore

        # So now we want to check the size of the chunks
        # We need to add in the sizes of the other dimensions so we calculate
        # the chunk size correctly
        # TODO - should we check if the specified chunk size is greater than the dimension size?
        MB_to_bytes = 1024 * 1024
        bytes_to_MB = 1 / MB_to_bytes

        current_chunksize = int(var_dtype_bytes * math.prod(chunks.values()))  # bytes
        max_chunk_size_bytes = max_chunk_size * MB_to_bytes

        if current_chunksize > max_chunk_size_bytes:
            # Do we want to check the secondary dimensions really?
            # if secondary_dimensions is not None:
            # raise NotImplementedError("Secondary dimensions scaling not yet implemented")
            # ratio = np.power(max_chunk_size / current_chunksize, 1 / len(secondary_dimensions))
            # for dim in secondary_dimensions:
            #     # Rescale chunks, but don't allow chunks smaller than 10
            #     chunks[dim] = max(int(ratio * chunks[dim]), 10)
            # else:
            raise ValueError(
                f"Chunk size {current_chunksize * bytes_to_MB} is greater than the maximum chunk size {max_chunk_size}"
            )

        # Do we need to supply the chunks of the other dimensions?
        # rechunk = {k: v for k, v in chunks.items() if v < dim_sizes[k]}
        # rechunk = {}
        # for k in dim_sizes:
        #     if chunks[k] < dim_sizes[k]:
        #         rechunk[k] = chunks.pop(k)
        return chunks
