""" This file contains the BaseModule class from which other processing
    modules inherit.
"""
from typing import Dict, List, Optional, Union, Type, TypeVar

__all__ = ["BaseModule"]

T = TypeVar("T", bound="BaseModule")


class BaseModule:
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
        from collections import defaultdict
        from openghg.util import to_defaultdict

        if not data:
            raise ValueError("Unable to create object with empty dictionary")

        if bucket is None:
            bucket = get_bucket()

        c = cls()
        c._creation_datetime = string_to_datetime(data["creation_datetime"])
        c._datasource_uuids = data["datasource_uuids"]
        c._datasource_names = data["datasource_names"]
        c._file_hashes = data["file_hashes"]
        c._datasource_table = to_defaultdict(data["datasource_table"])

        try:
            c._rank_data = to_defaultdict(data["rank_data"])
        except KeyError:
            c._rank_data = defaultdict(dict)

        c._stored = False

        return c

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

    def add_datasources(self: T, datasource_uuids: Dict, metadata: Dict) -> None:
        """Add the passed list of Datasources to the current list

        Args:
            datasource_uuids: Datasource UUIDs
            metadata: Metadata for each species
        Returns:
            None
        """
        for species, uid in datasource_uuids.items():
            metadata = metadata[species]

            site = metadata["site"]
            network = metadata["network"]
            inlet = metadata["inlet"]

            result = self._datasource_table[site][network][inlet][species]

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self._datasource_table[site][network][inlet][species] = uid

        print(self._datasource_table)

        self._datasource_names.update(datasource_uuids)
        # Invert the dictionary to update the dict keyed by UUID
        uuid_keyed = {v: k for k, v in datasource_uuids.items()}
        self._datasource_uuids.update(uuid_keyed)

    def datasources(self: T) -> List[str]:
        """Return the list of Datasources UUIDs associated with this object

        Returns:
            list: List of Datasource UUIDs
        """
        return list(self._datasource_uuids.keys())

    def datasource_names(self: T) -> Dict:
        """Return the names of the datasources

        Returns:
            dict: Dictionary of Datasource names
        """
        import warnings

        warnings.warn("This may be removed in a future release", category=DeprecationWarning)

        return self._datasource_names

    def remove_datasource(self: T, uuid: str) -> None:
        """Remove the Datasource with the given uuid from the list
        of Datasources

        Args:
            uuid (str): UUID of Datasource to be removed
        Returns:
            None
        """
        del self._datasource_uuids[uuid]

    def set_rank(self: T, uuid: str, rank: Union[int, str], daterange: Union[str, List[str]]) -> None:
        """Set the rank of a Datasource associated with this object.

        This function performs checks to ensure multiple ranks aren't set for
        overlapping dateranges.

        Passing a daterange and rank to this function will overwrite any current
        daterange stored for that rank.

        Args:
            uuid (str): UUID of Datasource
            rank (int): Rank of data
            daterange (str, list): Daterange(s)
        Returns:
            None
        """
        from openghg.modules import Datasource
        from openghg.util import daterange_from_str

        if not 0 <= int(rank) <= 10:
            raise TypeError("Rank can only take values 0 (for unranked) to 10. Where 1 is the highest rank.")

        if not isinstance(daterange, list):
            daterange = [daterange]

        try:
            rank_data = self._rank_data[uuid]
            # Check this source isn't ranked differently for the same dates
            for d in daterange:
                # Check we don't have any overlapping dateranges for other ranks
                daterange_obj = daterange_from_str(d)
                # Check the other dateranges for overlapping dates and raise error
                for existing_rank, existing_daterange in rank_data.items():
                    for e in existing_daterange:
                        e = daterange_from_str(e)

                        intersection = daterange_obj.intersection(e)
                        if len(intersection) > 0 and int(existing_rank) != int(rank):
                            raise ValueError(
                                f"This datasource has already got the rank {existing_rank} for dates that overlap the ones given. \
                                                Overlapping dates are {intersection}"
                            )
        except KeyError:
            pass

        # Store the rank within the Datasource
        datasource = Datasource.load(uuid=uuid, shallow=True)
        datasource.set_rank(rank=rank, daterange=daterange)
        datasource.save()

        try:
            self._rank_data[uuid][rank].extend(daterange)
        except KeyError:
            self._rank_data[uuid][rank] = daterange

    def clear_datasources(self: T) -> None:
        """Remove all Datasources from the object

        Returns:
            None
        """
        self._datasource_uuids.clear()
        self._datasource_names.clear()
        self._file_hashes.clear()

    def datasource_lookup(self: T, datasource_table) -> Dict:
        """Lookup the Datasources to store the associated data with


        
        """
