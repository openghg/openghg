import json
from io import BytesIO
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from openghg.dataobjects import ObsData
from openghg.store import recombine_datasets
from openghg.util import running_on_hub
from pandas import DataFrame
from xarray import Dataset, open_dataset

__all__ = ["SearchResults"]

T = TypeVar("T", bound="SearchResults")


class SearchResults:
    """This class is used to return data from the search function. It
    has member functions to retrieve data from the object store.

    Args:
        keys: Dictionary of keys keyed by Datasource UUID
        metadata: Dictionary of metadata keyed by Datasource UUID
    """

    def __init__(self, keys: Optional[Dict] = None, metadata: Optional[Dict] = None):
        if metadata is not None:
            self.metadata = metadata
        else:
            self.metadata = {}

        if metadata is not None:
            self.results = (
                DataFrame.from_dict(data=metadata, orient="index").reset_index().drop(columns="index")
            )
        else:
            self.results = {}

        if keys is not None:
            self.key_data = keys
        else:
            self.key_data = {}

        self.hub = running_on_hub()

    def __str__(self) -> str:
        return f"Found {len(self.results.index)} results.\nView the results DataFrame using the results property."

    def __repr__(self) -> str:
        return self.__str__()

    def __bool__(self) -> bool:
        return bool(self.metadata)

    def __len__(self) -> int:
        return len(self.metadata)

    # def __iter__(self) -> Iterator:
    #     yield from self.results.iterrows()

    def to_data(self) -> Dict:
        """Convert this object to a dictionary for JSON serialisation

        Returns:
            dict: Dictionary of data
        """
        return {
            "metadata": self.metadata,
            "keys": self.key_data,
            "hub": self.hub,
        }

    def to_json(self) -> str:
        """Serialises the object to JSON

        Returns:
            str: JSON str
        """
        return json.dumps(self.to_data())

    @classmethod
    def from_json(cls: Type[T], data: Union[bytes, str]) -> T:
        """Create a SearchResults object from a dictionary

        Args:
            data: Serialised object
        Returns:
            SearchResults: SearchResults object
        """
        loaded = json.loads(data)

        return cls(keys=loaded["keys"], metadata=loaded["metadata"])

    def retrieve(
        self,
        dataframe: Optional[DataFrame] = None,
        sort: bool = False,
        elevate_inlet: bool = False,
        **kwargs: Any,
    ) -> Union[ObsData, List[ObsData]]:
        """Retrieve data from object store using a filtered pandas DataFrame

        Args:
            dataframe: pandas DataFrame
            sort: Sort data by date in retrieved Dataset
            elevate_inlet: Elevate inlet to a variable within the Dataset, useful
            for ranked data.
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        if dataframe is not None:
            uuids = dataframe["uuid"].to_list()
            return self._retrieve_by_uuid(uuids=uuids, sort=sort, elevate_inlet=elevate_inlet)
        else:
            return self._retrieve_by_term(sort=sort, elevate_inlet=elevate_inlet, **kwargs)

    def retrieve_all(self, sort: bool = True, elevate_inlet: bool = False) -> Union[ObsData, List[ObsData]]:
        """Retrieves all data found during the search

        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        uuids = list(self.key_data.keys())
        return self._retrieve_by_uuid(uuids=uuids, sort=sort, elevate_inlet=elevate_inlet)

    def _retrieve_by_term(
        self, sort: bool, elevate_inlet: bool, **kwargs: Any
    ) -> Union[ObsData, List[ObsData]]:
        """Retrieve data from the object store by search term. This function scans the
        metadata of the retrieved results, retrieves the UUID associated with that data,
        pulls it from the object store, recombines it into an xarray Dataset and returns
        ObsData object(s).

        Args:
            sort: Pass the sort argument to the recombination function
            (sorts by time)
            elevate_inlet: Elevate the inlet variable in the Dataset to
            a variable (used for ranked data)
            kwargs: Metadata values to search for
        """
        uuids = set()
        # Make sure we don't have any Nones
        clean_kwargs = {k: v for k, v in kwargs.items() if v is not None}

        for uid, metadata in self.metadata.items():
            n_required = len(clean_kwargs)
            n_matched = 0
            for key, value in clean_kwargs.items():
                try:
                    # Here we want to check if it's a list and if so iterate over it
                    if isinstance(value, (list, tuple)):
                        for val in value:
                            val = str(val).lower()
                            if metadata[key.lower()] == val:
                                n_matched += 1
                                break
                    else:
                        value = str(value).lower()
                        if metadata[key.lower()] == value:
                            n_matched += 1
                except KeyError:
                    pass

            if n_matched == n_required:
                uuids.add(uid)

        # Now we can retrieve the data using the UUIDs
        return self._retrieve_by_uuid(uuids=list(uuids), sort=sort, elevate_inlet=elevate_inlet)

    def _retrieve_by_uuid(
        self, uuids: List, sort: bool, elevate_inlet: bool
    ) -> Union[ObsData, List[ObsData]]:
        """Internal retrieval function that uses the passed in UUIDs to retrieve
        the keys from the key_data dictionary, pull the data from the object store,
        create ObsData object(s) and return the result.

        Args:
            uuids: UUIDs of Datasources in the object store
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        results = []
        # For uid in uuids
        for uid in uuids:
            keys = self.key_data[uid]
            dataset = self._retrieve_dataset(keys, sort=sort, elevate_inlet=elevate_inlet)
            metadata = self.metadata[uid]

            results.append(ObsData(data=dataset, metadata=metadata))

        if len(results) == 1:
            return results[0]
        else:
            return results

    def _retrieve_dataset(
        self, keys: List, sort: bool, elevate_inlet: bool = True, attrs_to_check: Optional[Dict] = None
    ) -> Dataset:
        """Retrieves datasets from either cloud or local object store

        Args:
            keys: List of object store keys
            sort: Sort data on recombination
            elevate_inlet: Elevate inlet from attribute to variable
        Returns:
            Dataset:
        """
        from openghg.cloud import call_function

        if self.hub:
            to_post: Dict[str, Union[Dict, List, bool, str]] = {}
            to_post["keys"] = keys
            to_post["sort"] = sort
            to_post["elevate_inlet"] = elevate_inlet
            to_post["function"] = "retrieve"

            if attrs_to_check is not None:
                to_post["attrs_to_check"] = attrs_to_check

            result = call_function(data=to_post)
            binary_netcdf = result["content"]["data"]
            buf = BytesIO(binary_netcdf)
            # TODO - remove this ignore once xarray have updated their type hints
            ds: Dataset = open_dataset(buf).load()  # type: ignore
            return ds
        else:
            return recombine_datasets(
                keys=keys, sort=sort, elevate_inlet=elevate_inlet, attrs_to_check=attrs_to_check
            )
