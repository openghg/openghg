""" This file contains the BaseModule class from which other processing
    modules inherit.
"""
from typing import Dict, List, Optional, Union, Type, TypeVar
from pandas import Timestamp

__all__ = ["BaseModule"]

T = TypeVar("T", bound="BaseModule")


class BaseModule:
    def __init__(self):
        from openghg.util import timestamp_now
        from addict import Dict as aDict

        self._creation_datetime = timestamp_now()
        self._stored = False

        # Use an addict Dict here for easy nested data storage
        self._datasource_table = aDict()
        # Keyed by Datasource UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Keyed by UUID
        self._rank_data = aDict()

    def is_null(self):
        return not self.datasources

    @classmethod
    def exists(cls: Type[T], bucket: Optional[str] = None) -> bool:
        """Check if a GC object is already saved in the object
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

        return exists(bucket=bucket, key=key)

    @classmethod
    def from_data(cls: Type[T], data: str, bucket: Optional[Dict] = None) -> T:
        """Create an object from data

        Args:
            data: JSON data
            bucket: Bucket for data storage
        Returns:
            cls: Class object of cls type
        """
        from Acquire.ObjectStore import string_to_datetime
        from openghg.objectstore import get_bucket
        from addict import Dict as aDict

        if not data:
            raise ValueError("Unable to create object with empty dictionary")

        if bucket is None:
            bucket = get_bucket()

        c = cls()
        c._creation_datetime = string_to_datetime(data["creation_datetime"])
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
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
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

        return cls.from_data(data=data, bucket=bucket)

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

    def set_rank(
        self: T, uuid: str, rank: Union[int, str], date_range: Union[str, List[str]], overwrite: Optional[bool] = False
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
        from openghg.util import (
            combine_dateranges,
            daterange_overlap,
            valid_daterange,
            trim_daterange,
            daterange_contains,
            split_encompassed_daterange,
        )

        from copy import deepcopy

        rank = int(rank)

        if not 1 <= rank <= 10:
            raise TypeError("Rank can only take values 1 to 10 (for unranked). Where 1 is the highest rank.")

        if not isinstance(date_range, list):
            date_range = [date_range]

        # Used to store dateranges that need to be trimmed to ensure no daterange overlap
        to_update = []
        # Non-overlapping dateranges that can be stored directly
        to_add = []

        if uuid in self._rank_data:
            rank_data = self._rank_data[uuid]
            # Check this source isn't ranked differently for the same dates
            for new_daterange in date_range:
                if not valid_daterange(new_daterange):
                    raise ValueError("Invalid daterange, please ensure start and end dates are correct.")

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
                            to_combine = (new_daterange, existing_daterange)
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
                    del ranking_backup[existing]
                    # If we want to overwrite an existing rank we need to trim that daterange and
                    # rewrite it back to the dictionary
                    rank_copy = rank_data[existing]
                    if overwrite:
                        # Here we trim the existing daterange down and reassign its rank
                        # If the existing daterange contains the new we need to split it out, save the old rank chunks
                        # and add the new daterangea and rank in.
                        if daterange_contains(container=existing, contained=new):
                            result = split_encompassed_daterange(container=existing, contained=new)

                            existing_start = result["container_start"]
                            ranking_backup[existing_start] = rank_copy

                            updated_new = result["contained"]
                            ranking_backup[updated_new] = rank

                            existing_end = result["container_end"]
                            ranking_backup[existing_end] = rank_copy
                        # If the new daterange contains the existing we can just overwrite it
                        elif daterange_contains(container=new, contained=existing):
                            ranking_backup[new] = rank_copy
                        else:
                            trimmed = trim_daterange(to_trim=existing, overlapping=new)
                            ranking_backup[trimmed] = rank_copy
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

    def clear_datasources(self: T) -> None:
        """Remove all Datasources from the object

        Returns:
            None
        """
        self._datasource_table.clear()
        self._datasource_uuids.clear()
        self._file_hashes.clear()
