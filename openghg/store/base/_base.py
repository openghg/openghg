""" This file contains the BaseStore class from which other retrieve
    modules inherit.
"""
from typing import Dict, List, Optional, Union, Type, TypeVar
from pandas import Timestamp

__all__ = ["BaseStore"]

T = TypeVar("T", bound="BaseStore")


class BaseStore:
    _root = "root"
    _uuid = "root_uuid"

    def __init__(self) -> None:
        from openghg.util import timestamp_now
        from addict import Dict as aDict

        self._creation_datetime = timestamp_now()
        self._stored = False

        # Use an addict Dict here for easy nested data storage
        self._datasource_table = aDict()
        # Keyed by Datasource UUID
        self._datasource_uuids: Dict[str, str] = {}
        # Hashes of previously uploaded files
        self._file_hashes: Dict[str, str] = {}
        # Keyed by UUID
        self._rank_data = aDict()

    def is_null(self) -> bool:
        return not self.datasources

    @classmethod
    def exists(cls: Type[T], bucket: Optional[str] = None) -> bool:
        """Check if the object is already saved in the object
        store

        Args:
            bucket: Bucket for data storage
        Returns:
            bool: True if object exists
        """
        from openghg.objectstore import exists, get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = f"{cls._root}/uuid/{cls._uuid}"

        does_exist: bool = exists(bucket=bucket, key=key)

        return does_exist

    @classmethod
    def from_data(cls: Type[T], data: Dict) -> T:
        """Create an object from data

        Args:
            data: JSON data
        Returns:
            cls: Class object of cls type
        """
        from openghg.util import timestamp_tzaware
        from addict import Dict as aDict

        if not data:
            raise ValueError("Unable to create object with empty dictionary")

        c = cls()
        c._creation_datetime = timestamp_tzaware(data["creation_datetime"])
        c._datasource_uuids = data["datasource_uuids"]
        c._file_hashes = data["file_hashes"]
        c._datasource_table = aDict(data["datasource_table"])
        c._rank_data = aDict(data["rank_data"])
        c._stored = False

        return c

    def to_data(self) -> Dict:
        """Return a JSON-serialisable dictionary of object
        for storage in object store

        Returns:
            dict: Dictionary version of object
        """
        data: Dict[str, Union[str, bool, Dict]] = {}
        data["creation_datetime"] = str(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_table"] = self._datasource_table
        data["datasource_uuids"] = self._datasource_uuids
        data["file_hashes"] = self._file_hashes
        data["rank_data"] = self._rank_data

        return data

    @classmethod
    def load(cls: Type[T], bucket: Optional[str] = None) -> T:
        """Load an object from the datastore using the passed
        bucket and UUID

        Args:
            bucket: Bucket to store object
        Returns:
            class: Class created from JSON data
        """
        from openghg.objectstore import get_bucket, get_object_from_json

        if not cls.exists():
            return cls()

        if bucket is None:
            bucket = get_bucket()

        key = f"{cls._root}/uuid/{cls._uuid}"
        data = get_object_from_json(bucket=bucket, key=key)

        return cls.from_data(data=data)

    def save(cls) -> None:
        """Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{cls._root}/uuid/{cls._uuid}"

        cls._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=cls.to_data())

    @classmethod
    def uuid(cls: Type[T]) -> str:
        """Return the UUID of this object

        Returns:
            str: UUID of object
        """
        return cls._uuid

    def datasources(self: T) -> List[str]:
        """Return the list of Datasources UUIDs associated with this object

        Returns:
            list: List of Datasource UUIDs
        """
        return list(self._datasource_uuids.keys())

    def remove_datasource(self: T, uuid: str) -> None:
        """Remove the Datasource with the given uuid from the list
        of Datasources

        Args:
            uuid (str): UUID of Datasource to be removed
        Returns:
            None
        """
        del self._datasource_uuids[uuid]

    def get_rank(self: T, uuid: str, start_date: Timestamp, end_date: Timestamp) -> Dict:
        """Get the rank for the given Datasource for a given date range

        Args:
            uuid: UUID of Datasource
            start_date: Start date
            end_date: End date
        Returns:
            dict: Dictionary of rank and daterange covered by that rank
        """
        from openghg.util import daterange_overlap, create_daterange_str
        from collections import defaultdict

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

    def clear_rank(self: T, uuid: str) -> None:
        """Clear the ranking data for a Datasource

        Args:
            uuid: UUID of Datasource
        Returns:
            None
        """
        if uuid in self._rank_data:
            del self._rank_data[uuid]
            self.save()
        else:
            raise ValueError("No ranking data set for that UUID.")

    def set_rank(
        self: T,
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
        from copy import deepcopy
        from openghg.util import (
            combine_dateranges,
            daterange_overlap,
            trim_daterange,
            daterange_contains,
            split_encompassed_daterange,
            sanitise_daterange,
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

    def rank_data(self: T) -> Dict:
        """Return a dictionary of rank data keyed
        by UUID

            Returns:
                dict: Dictionary of rank data
        """
        rank_dict: Dict = self._rank_data.to_dict()
        return rank_dict

    def clear_datasources(self: T) -> None:
        """Remove all Datasources from the object

        Returns:
            None
        """
        self._datasource_table.clear()
        self._datasource_uuids.clear()
        self._file_hashes.clear()
