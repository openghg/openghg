from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from typing import Any

DatasourceT = TypeVar("DatasourceT", bound="AbstractDatasource")


UUID = str
Bucket = str


class DatasourceFactory(Generic[DatasourceT]):
    """Class that allows Datasources to be created using only UUIDs.

    Any other arguments needed to initialise or load a Datasource are
    passed to the init method of this class. Passing a DatasourceFactory
    object to a function or class allows Datasources to be used without
    knowledge of how they are stored.
    """

    def __init__(
        self,
        datasource_class: type[DatasourceT],
        new_kwargs: dict[str, Any] | None = None,
        load_kwargs: dict[str, Any] | None = None,
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

    def new(self, uuid: UUID) -> DatasourceT:
        """Initialise new `Datasource` via UUID."""
        return self.datasource_class(uuid=uuid, **self.new_kwargs)

    def load(self, uuid: UUID) -> DatasourceT:
        """Load existing `Datasource` via UUID."""
        return self.datasource_class.load(uuid=uuid, **self.load_kwargs)


T = TypeVar("T")


class AbstractDatasource(ABC, Generic[T]):
    """Interface for Datasource-like objects.

    The data stored in a Datasource is assumed to be related in some way.

    For instance, a data source might contain time series data for concentrations
    of a particular gas, measured from a specific instrument, at a specific
    inlet height, and at a specific site.

    Datasources are stored by UUID, and must have a `load` classmethod
    to support this.
    """

    def __init__(self, uuid: UUID, *args: Any, **kwargs: Any) -> None:
        self.uuid = uuid

    @classmethod
    @abstractmethod
    def load(cls: type[DatasourceT], uuid: UUID, *args: Any, **kwargs: Any) -> DatasourceT:
        pass

    @abstractmethod
    def add(self, data: T, *args, **kwargs: Any) -> None:
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
