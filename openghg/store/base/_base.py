""" This file contains the BaseStore class from which other storage
    modules inherit.
"""
from typing import Any, Dict, List, Optional, Sequence, TypeVar, Union
from pandas import Timestamp
import tinydb
import logging
from openghg.types import DatasourceLookupError
from openghg.objectstore import get_object_from_json, exists, set_object_from_json
from openghg.util import timestamp_now


T = TypeVar("T", bound="BaseStore")

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BaseStore:
    _root = "root"
    _uuid = "root_uuid"

    def __init__(self, bucket: str) -> None:
        from openghg.store import load_metastore

        self._creation_datetime = str(timestamp_now())
        self._stored = False
        # Keyed by Datasource UUID
        self._datasource_uuids: Dict[str, str] = {}
        # Hashes of previously uploaded files
        self._file_hashes: Dict[str, str] = {}
        # Hashes of previously stored data from other data platforms
        self._retrieved_hashes: Dict[str, Dict] = {}
        # Where we'll store this object
        self._bucket = bucket
        self._metakey = ""
        self._metastore = load_metastore(bucket=bucket, key=self.metakey())

        if exists(bucket=bucket, key=self.key()):
            data = get_object_from_json(bucket=bucket, key=self.key())
            # Update myself
            self.__dict__.update(data)

    @classmethod
    def metakey(cls) -> str:
        return str(cls._metakey)

    @classmethod
    def key(cls) -> str:
        return f"{cls._root}/uuid/{cls._uuid}"

    def save(self) -> None:
        self._metastore.close()
        set_object_from_json(bucket=self._bucket, key=self.key(), data=self.to_data())

    def to_data(self) -> Dict:
        # We don't need to store the metadata store, it has its own location
        # QUESTION - Is this cleaner than the previous specifying
        DO_NOT_STORE = ["_metastore"]
        return {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}

    def assign_data(
        self,
        data: Dict,
        overwrite: bool,
        data_type: str,
        required_keys: Sequence[str],
        min_keys: Optional[int] = None,
        update_keys: Optional[List] = None,
    ) -> Dict[str, Dict]:
        """Assign data to a Datasource. This will either create a new Datasource
        Create or get an existing Datasource for each gas in the file

            Args:
                data: Dictionary containing data and metadata for species
                overwrite: If True overwrite current data stored
                data_type: Type of data, timeseries etc
                required_keys: Required minimum keys to lookup unique Datasource
                min_keys: Minimum number of metadata keys needed to uniquely match a Datasource
            Returns:
                dict: Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
        """
        from openghg.store.base import Datasource
        #from openghg.util import to_lowercase

        uuids = {}

        lookup_results = self.datasource_lookup(data=data, required_keys=required_keys, min_keys=min_keys)
        # TODO - remove this when the lowercasing of metadata gets removed
        # We currently lowercase all the metadata and some keys we don't want to change, such as paths to the object store
        skip_keys = ["object_store"]

        for key, parsed_data in data.items():
            metadata = parsed_data["metadata"]
            _data = parsed_data["data"]

            # Our lookup results and gas data have the same keys
            uuid = lookup_results[key]

            # Add the read metadata to the Dataset attributes being careful
            # not to overwrite any attributes that are already there
            def convert_to_netcdf4_types(value: Any) -> Union[int, float, str, list]:
                """Attributes in a netCDF file can be strings, numbers, or sequences:
                http://unidata.github.io/netcdf4-python/#attributes-in-a-netcdf-file

                This function converts any data whose type is not int, float, str, or list
                to strings.
                Booleans are converted to strings, even though they are a subtype of int.
                """
                if isinstance(value, (int, float, str, list)) and not isinstance(value, bool):
                    return value
                else:
                    return str(value)

            to_add = {k: convert_to_netcdf4_types(v) for k, v in metadata.items() if k not in _data.attrs}
            _data.attrs.update(to_add)

            # If we have a UUID for this Datasource load the existing object
            # from the object store
            # If we haven't stored data with this metadata before we create a new Datasource
            # and add the metadata to our metastore

            # Take a copy of the metadata so we can update it
            meta_copy = metadata.copy()

            new_ds = uuid is False

            if new_ds:
                datasource = Datasource()
                uid = datasource.uuid()
                meta_copy["uuid"] = uid
                # For retrieval later we'll need to know which bucket this is stored in
                meta_copy["object_store"] = self._bucket

                # Make sure all the metadata is lowercase for easier searching later
                # TODO - do we want to do this or should be just perform lowercase comparisons?
                # meta_copy = to_lowercase(d=meta_copy, skip_keys=skip_keys)
                # TODO - 2023-05-25 - Remove the need for this key, this should just be a set
                # so we can have rapid
                self._datasource_uuids[uid] = key
            else:
                datasource = Datasource.load(bucket=self._bucket, uuid=uuid)

            # Add the dataframe to the datasource
            datasource.add_data(
                metadata=meta_copy, data=_data, overwrite=overwrite, data_type=data_type, skip_keys=skip_keys
            )
            # Save Datasource to object store
            datasource.save(bucket=self._bucket)

            # Add the metadata to the metastore and make sure it's up to date with the metadata stored
            # in the Datasource
            datasource_metadata = datasource.metadata()
            if new_ds:
                self._metastore.insert(datasource_metadata)
            else:
                self._metastore.update(datasource_metadata, tinydb.where("uuid") == datasource.uuid())

            uuids[key] = {"uuid": datasource.uuid(), "new": new_ds}

        return uuids

    def datasource_lookup(
        self, data: Dict, required_keys: Sequence[str], min_keys: Optional[int] = None
    ) -> Dict:
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
        if min_keys is None:
            min_keys = len(required_keys)

        results = {}
        for key, _data in data.items():
            metadata = _data["metadata"]

            def lower_if_string(val):
                "Convert strings to lower case, leave types alone."
                if isinstance(val, str):
                    return val.lower()
                else:
                    return val

            required_metadata = {
                k.lower(): lower_if_string(v) for k, v in metadata.items() if k in required_keys
            }

            if len(required_metadata) < min_keys:
                raise ValueError(
                    f"The given metadata doesn't contain enough information, we need: {required_keys}"
                )

            required_result = self._metastore.search(tinydb.Query().fragment(required_metadata))

            if not required_result:
                results[key] = False
            elif len(required_result) > 1:
                raise DatasourceLookupError("More than once Datasource found for metadata, refine lookup.")
            else:
                results[key] = required_result[0]["uuid"]

        return results

    def uuid(self) -> str:
        """Return the UUID of this object

        Returns:
            str: UUID of object
        """
        return self._uuid

    def datasources(self) -> List[str]:
        """Return the list of Datasources UUIDs associated with this object

        Returns:
            list: List of Datasource UUIDs
        """
        return list(self._datasource_uuids.keys())

    def remove_datasource(self, uuid: str) -> None:
        """Remove the Datasource with the given uuid from the list
        of Datasources

        Args:
            uuid: UUID of Datasource to be removed
        Returns:
            None
        """
        del self._datasource_uuids[uuid]

    def get_rank(self, uuid: str, start_date: Timestamp, end_date: Timestamp) -> Dict:
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
        rank: Union[int, str],
        date_range: Union[str, List[str]],
        overwrite: Optional[bool] = False,
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

    def rank_data(self) -> Dict:
        """Return a dictionary of rank data keyed
        by UUID

            Returns:
                dict: Dictionary of rank data
        """
        raise NotImplementedError("Ranking is being reworked and will be reactivated in a future release.")
        rank_dict: Dict = self._rank_data.to_dict()
        return rank_dict

    def clear_datasources(self) -> None:
        """Remove all Datasources from the object

        Returns:
            None
        """
        self._datasource_uuids.clear()
        self._file_hashes.clear()
