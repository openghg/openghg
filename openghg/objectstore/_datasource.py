from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar
from typing import Any, List, Optional


DS = TypeVar("DS", bound='Datasource')


UUID = str
Data = Any
Bucket = str


class Datasource(ABC):
    """Interface for Datasource-like objects.

    The data stored in a Datasource is assumed to be related in some way.

    For instance, a data source might contain time series data for concentrations
    of a particular gas, measured from a specific instrument, at a specific
    inlet height, and at a specific site.

    Datasources are stored by UUID within buckets, and must have a `load` classmethod
    to support this.
    """
    def __init__(self, uuid: UUID) -> None:
        self.uuid = uuid

    @classmethod
    @abstractmethod
    def load(cls: type[DS], bucket: Bucket, uuid: UUID) -> DS:
        pass

    @abstractmethod
    def add(self, data: Data) -> None:
        """Add data to the datasource.

        TODO: add `overwrite` argument, with expected error type
        if trying to overwrite data without saying so.
        """
        pass

    @abstractmethod
    def delete(self) -> None:
        """Delete all of the data stored by this datasource."""
        pass

    @abstractmethod
    def save(self, bucket: Bucket) -> None:
        """Save changes to datasource made by `add` method."""
        pass


T = TypeVar('T', bound='InMemoryDatasource')


class InMemoryDatasource(Datasource):
    """Minimal class implementing the AbstractDatasource interface."""
    datasources: dict[UUID, List[Data]] = dict()

    def __init__(self, uuid: UUID, data: Optional[List[Data]] = None) -> None:
        self.uuid = uuid
        if data:
            self.data: List[Data] = data
        else:
            self.data: List[Data] = []

    @classmethod
    def load(cls: type[T], bucket: Bucket, uuid: UUID) -> T:
        try:
            data = cls.datasources[uuid]
        except KeyError:
            raise LookupError(f'No datasource with UUID {uuid} found in bucket {bucket}.')
        else:
            return cls(uuid, data)

    def add(self, data: Data) -> None:
        self.data.append(data)

    def delete(self) -> None:
        self.data = []
        del InMemoryDatasource.datasources[self.uuid]

    def save(self, bucket: Bucket) -> None:
        InMemoryDatasource.datasources[self.uuid] = self.data
