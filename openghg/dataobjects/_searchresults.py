from collections.abc import Hashable, Iterator, Iterable
import logging
from typing import Any, cast, Optional, Union

import pandas as pd
import tinydb

from openghg.dataobjects._basedata import BaseData
from openghg.objectstore import DataObject
from openghg.util import running_on_hub


__all__ = ["SearchResults"]

logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class SearchResults:
    """This class is used to return data from the search function.

    Printing a `SearchResults` object displays a table of results.

    The `.results` attribute contains a pandas DataFrame with the metadata
    of the search results.

    Filtering the results DataFrame and passing the filtered DataFrame to
    the `.retrive()` method will retrieve the data for those results.

    The `.retrieve_all()` method retrieves data for all search results.
    """

    def __init__(
        self,
        data_objects: Iterable[DataObject],
        start_result: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> None:
        """Create a SearchResults object.

        Args:
            data_objects: Iterable (e.g. list) of DataObjects.
            start_result: key to use in first column of `results` DataFrame
            start_date: start date to slice data to when retrieving
            end_date: end date to slice data to when retrieving

        Returns:
            None
        """
        self.start_result = start_result
        self.data_objects = data_objects or []

        self._start_date = pd.to_datetime(start_date) if start_date else None
        self._end_date = pd.to_datetime(end_date) if end_date else None

        self._results = None

        self.hub = running_on_hub()

    def to_dict(self) -> dict:
        return {do.uuid: do.metadata for do in self.data_objects}

    @property
    def results(self) -> pd.DataFrame:
        if self._results is not None:
            # HACK to maintain behavior of old code
            if self._results.empty:
                return {}  # type: ignore
            return self._results

        df = pd.DataFrame.from_dict(self.to_dict(), orient="index").reset_index(drop=True)

        if self.start_result is not None and self.start_result in df.columns:
            cols = list(df.columns)
            cols.remove(self.start_result)
            cols = [self.start_result, *cols]
            df = cast(pd.DataFrame, df[cols])

        self._results = df

        # HACK to maintain behavior of old code
        if df.empty:
            return {}  # type: ignore

        return df

    def __str__(self) -> str:
        SearchResults.df_to_table_console_output(df=pd.DataFrame.from_dict(data=self.to_dict()))

        return (
            f"Found {len(self.results)} results.\nView the results pd.DataFrame using the results property."
        )

    def __repr__(self) -> str:
        return f"SearchResults({self.data_objects}, start_result={self.start_result}, start_date={self._start_date}, end_date={self._end_date})"

    def __bool__(self) -> bool:
        return bool(self.data_objects)

    def __len__(self) -> int:
        return len(self.data_objects)

    def __iter__(self) -> Iterator:
        return iter(self.data_objects)

    # NOTE: these get/set item methods would probably be faster if we stored the data by UUID.
    # These methods are needed by `DataManager`, and eventually, it would be better if DataObjects could
    # be updated directly.

    def __getitem__(self, key: Hashable) -> DataObject:
        for do in self.data_objects:
            if key in (do, do.uuid):
                return do
        raise KeyError(f"Item with key {key} not found in SearchResults.")

    def __setitem__(self, key: Hashable, value: DataObject) -> None:
        """Set value based on UUID."""
        if key not in self.uuids:
            self.data_objects.append(value)
        else:
            old = next(do for do in self.data_objects if do.uuid == key)
            self.data_objects.remove(old)
            self.data_objects.append(value)

    def __contains__(self, value: Union[str, DataObject]) -> bool:
        """Return True if `value` is a DataObject or UUID of a DataObject in the SearchResults."""
        return any(value in (do, do.uuid) for do in self)

    def retrieve(
        self,
        dataframe: Optional[pd.DataFrame] = None,
        version: str = "latest",
        sort: bool = True,
        **kwargs: Any,
    ) -> Union[BaseData, list[BaseData]]:
        """Retrieve data from object store using a filtered pandas pd.DataFrame

        Args:
            dataframe: pandas pd.DataFrame
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort data by time in retrieved Dataset
            **kwargs: Metadata values to search for
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        if dataframe is not None:
            uuids = dataframe["uuid"].to_list()
            return self._retrieve_by_uuid(uuids=uuids, version=version, sort=sort)
        else:
            return self._retrieve_by_term(version=version, sort=sort, **kwargs)

    def retrieve_all(
        self,
        version: str = "latest",
        sort: bool = True,
    ) -> Union[BaseData, list[BaseData]]:
        """Retrieves all data found during the search

        Args:
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        result = [
            BaseData.from_data_object(
                do, version=version, start_date=self._start_date, end_date=self._end_date, sort=sort
            )
            for do in self.data_objects
        ]

        if len(result) == 1:
            return result[0]
        return result

    @property
    def uuids(self) -> list[str]:
        """Return the UUIDs of the found data

        Returns:
            list: List of UUIDs
        """
        return [do.uuid for do in self.data_objects]

    def _retrieve_by_term(
        self, version: str, sort: bool = True, **kwargs: Any
    ) -> Union[BaseData, list[BaseData]]:
        """Retrieve data from the object store by search term. This function scans the
        metadata of the retrieved results, retrieves the UUID associated with that data,
        pulls it from the object store, recombines it into an xarray Dataset and returns
        BaseData object(s).

        Args:
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
            **kwargs: Metadata values to search for
        """
        db = tinydb.TinyDB(storage=tinydb.storages.MemoryStorage)
        for do in self.data_objects:
            db.insert(do)

        # Make sure we don't have any Nones
        clean_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        basic_kwargs = {k: v for k, v in clean_kwargs.items() if not isinstance(v, (list, tuple))}
        query = tinydb.Query().fragment(basic_kwargs)

        # process tuple and list values
        for k, v in clean_kwargs.items():
            if k not in basic_kwargs:
                list_query = tinydb.Query()[k].one_of([str(x).lower() for x in v])
                query = query & list_query

        uuids = [doc["uuid"] for doc in db.search(query)]

        return self._retrieve_by_uuid(uuids=uuids, version=version, sort=sort)

    def _retrieve_by_uuid(
        self, uuids: Iterable[str], version: str = "latest", sort: bool = True
    ) -> Union[BaseData, list[BaseData]]:
        """Internal retrieval function that uses the passed in UUIDs to retrieve
        the keys from the key_data dictionary, pull the data from the object store,
        create ObsData object(s) and return the result.

        Args:
            uuids: UUIDs of Datasources in the object store
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
        Returns:
            BaseData / List[BaseData]: BaseData object(s)
        """
        results = []
        data_objects = [do for do in self.data_objects if do.uuid in uuids]
        results = [
            BaseData.from_data_object(
                do, version=version, sort=sort, start_date=self._start_date, end_date=self._end_date
            )
            for do in data_objects
        ]

        if len(results) == 1:
            return results[0]
        else:
            return results

    @staticmethod
    def df_to_table_console_output(df: pd.DataFrame) -> None:
        """
        Process the pd.DataFrame and display it as a formatted table in the console.

        Args:
            df (pd.DataFrame): The DataFrame to be processed and displayed.

        Returns:
            None
        """
        try:
            from rich import print
            from rich.table import Table, box
        except ModuleNotFoundError:
            logger.warning("Unable to use rich package to display search results. Please install rich")
            return None

        # Split columns into sets
        column_sets = [df.columns[i : i + 4] for i in range(0, len(df.columns), 4)]

        # Iterate over the column sets
        for columns in column_sets:
            # Create a table instance
            table = Table(show_header=False, header_style="bold", box=box.HORIZONTALS)

            # Add table headers
            for i, column in enumerate(columns, start=1):
                if i == 1:
                    table.add_column(column, style="bold")
                else:
                    table.add_column(column)

            # Add table data
            for index, row in df.iterrows():
                row_data = [str(index)] + [str(row[column]) for column in columns]
                table.add_row(*row_data)

            # Print the table
            print(table)
            print()
