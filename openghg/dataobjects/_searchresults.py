import logging
from typing import Any, TypeVar
from collections.abc import Iterable

from openghg.dataobjects import ObsData
from pandas import DataFrame

__all__ = ["SearchResults"]

T = TypeVar("T", bound="SearchResults")

logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class SearchResults:
    """This class is used to return data from the search function. It
    has member functions to retrieve data from the object store.

    Args:
        keys: Dictionary of keys keyed by Datasource UUID
        metadata: Dictionary of metadata keyed by Datasource UUID
        start_result: ?
    """

    # TODO - WIP move to tinydb metadata lookup to simplify code
    def __init__(
        self,
        metadata: dict | None = None,
        start_result: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        # db = tinydb.TinyDB(tinydb.storages.MemoryStorage)
        if metadata is not None:
            self.metadata = metadata
            # db.insert_multiple([m for m in metadata.values()])
            self.results = (
                DataFrame.from_dict(data=metadata, orient="index").reset_index().drop(columns="index")
            )

            if start_result is not None:
                for uuid_key, uuid_metadata in metadata.items():
                    if start_result in uuid_metadata:
                        other_keys = list(uuid_metadata.keys())
                        other_keys.remove(start_result)
                        reorder = [start_result] + other_keys
                        metadata[uuid_key] = {key: uuid_metadata[key] for key in reorder}

        else:
            self.results = {}  # type: ignore
            self.metadata = {}

        self._start_date = start_date
        self._end_date = end_date

    def __str__(self) -> str:
        SearchResults.df_to_table_console_output(df=DataFrame.from_dict(data=self.metadata))

        return f"Found {len(self.results)} results.\nView the results DataFrame using the results property."

    def __repr__(self) -> str:
        return self.__str__()

    def __bool__(self) -> bool:
        return bool(self.metadata)

    def __len__(self) -> int:
        return len(self.metadata)

    # def __iter__(self) -> Iterator:
    #     yield from self.results.iterrows()

    def retrieve(
        self,
        dataframe: DataFrame | None = None,
        version: str = "latest",
        sort: bool = True,
        **kwargs: Any,
    ) -> ObsData | list[ObsData]:
        """Retrieve data from object store using a filtered pandas DataFrame

        Args:
            dataframe: pandas DataFrame
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
    ) -> ObsData | list[ObsData]:
        """Retrieves all data found during the search

        Args:
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        return self._retrieve_by_uuid(uuids=self.metadata.keys(), version=version, sort=sort)

    def uuids(self) -> list:
        """Return the UUIDs of the found data

        Returns:
            list: List of UUIDs
        """
        return list(self.metadata.keys())

    def _retrieve_by_term(self, version: str, sort: bool = True, **kwargs: Any) -> ObsData | list[ObsData]:
        """Retrieve data from the object store by search term. This function scans the
        metadata of the retrieved results, retrieves the UUID associated with that data,
        pulls it from the object store, recombines it into an xarray Dataset and returns
        ObsData object(s).

        Args:
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
            **kwargs: Metadata values to search for
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
        return self._retrieve_by_uuid(uuids=list(uuids), version=version, sort=sort)

    def _retrieve_by_uuid(self, uuids: Iterable, version: str, sort: bool = True) -> ObsData | list[ObsData]:
        """Internal retrieval function that uses the passed in UUIDs to retrieve
        the keys from the key_data dictionary, pull the data from the object store,
        create ObsData object(s) and return the result.

        Args:
            uuids: UUIDs of Datasources in the object store
            version: Version of data requested from Datasource. Default = "latest".
            sort: Sort by time. Note that this may be very memory hungry for large Datasets.
        Returns:
            ObsData / List[ObsData]: ObsData object(s)
        """
        results = []
        for uuid in uuids:
            metadata = self.metadata[uuid]
            if version == "latest":
                version = metadata["latest_version"]
            else:
                if version not in metadata["versions"]:
                    raise ValueError(f"Invalid version {version} for UUID {uuid}")

            results.append(
                ObsData(
                    uuid=uuid,
                    version=version,
                    metadata=metadata,
                    start_date=self._start_date,
                    end_date=self._end_date,
                    sort=sort,
                )
            )

        if len(results) == 1:
            return results[0]
        else:
            return results

    @staticmethod
    def df_to_table_console_output(df: DataFrame) -> None:
        """
        Process the DataFrame and display it as a formatted table in the console.

        Args:
            df (DataFrame): The DataFrame to be processed and displayed.

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
