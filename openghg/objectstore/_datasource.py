from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from typing import Any, List, Optional


DS = TypeVar("DS", bound="Datasource")


UUID = str
Data = Any
Bucket = str


class DatasourceFactory(Generic[DS]):
    """Class that allows Datasources to be created using only UUIDs.

    Any other arguments needed to initialise or load a Datasource are
    passed to the init method of this class. Passing a DatasourceFactory
    object to a function or class allows Datasources to be used without
    knowledge of how they are stored.
    """
    def __init__(
        self,
        datasource_class: type[DS],
        new_kwargs: Optional[dict[str, Any]] = None,
        load_kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialise DatasourceFactory.

        Args:
            datasource_class: type of `Datasource` to be created by factory.
            new_kwargs: dict of keyword args to pass to `__init__` method of
                `Datasource` class.
            load_kwargs: dict of keyword args to pass to `load` method of
                `Datasource` class.
        """
        self.datasource_class = datasource_class
        self.new_kwargs = new_kwargs or {}
        self.load_kwargs = load_kwargs or {}

    def new(self, uuid: UUID) -> DS:
        """Initialise new `Datasource` via UUID."""
        return self.datasource_class(uuid=uuid, **self.new_kwargs)

    def load(self, uuid: UUID) -> DS:
        """Load existing `Datasource` via UUID."""
        return self.datasource_class.load(uuid=uuid, **self.load_kwargs)


class Datasource(ABC):
    """Interface for Datasource-like objects.

    The data stored in a Datasource is assumed to be related in some way.

    For instance, a data source might contain time series data for concentrations
    of a particular gas, measured from a specific instrument, at a specific
    inlet height, and at a specific site.

    Datasources are stored by UUID, and must have a `load` classmethod
    to support this.
    """

    def __init__(self, uuid: UUID) -> None:
        self.uuid = uuid

    @classmethod
    @abstractmethod
    def load(cls: type[DS], uuid: UUID) -> DS:
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
    def save(self) -> None:
        """Save changes to datasource made by `add` method."""
        pass


T = TypeVar("T", bound="InMemoryDatasource")


class InMemoryDatasource(Datasource):
    """Minimal class implementing the Datasource interface."""

    datasources: dict[UUID, List[Data]] = {}

    def __init__(self, uuid: UUID, data: Optional[List[Data]] = None) -> None:
        super().__init__(uuid)
        if data:
            self.data: List[Data] = data
        else:
            self.data: List[Data] = []

    @classmethod
    def load(cls: type[T], uuid: UUID) -> T:
        try:
            data = cls.datasources[uuid]
        except KeyError:
            raise LookupError(f"No datasource with UUID {uuid} found.")
        else:
            return cls(uuid, data)

    def add(self, data: Data) -> None:
        self.data.append(data)

    def delete(self) -> None:
        self.data = []
        del InMemoryDatasource.datasources[self.uuid]

    def save(self) -> None:
        InMemoryDatasource.datasources[self.uuid] = self.data
