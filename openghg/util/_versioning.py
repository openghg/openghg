from collections import UserDict, UserList
from collections.abc import Callable, Iterable
from copy import deepcopy
from typing import Any, Generic, Protocol, runtime_checkable, TypeVar
from collections.abc import Mapping


@runtime_checkable
class HasDelete(Protocol):
    """Satisfied by classes with `delete` method."""

    def delete(self) -> None: ...

    """Delete self."""


class VersionError(Exception): ...


T = TypeVar("T")  # underlying class type


class SimpleVersioning(Generic[T]):
    """Work with versions of an object.

    This can be used to create a "versioned" subclass of the class T in some circumstances, by
    using `SimpleVersioning` like a mixin.

    For instance, we could make a "versioned list" class by inheriting from `SimpleVersioning[str, list]` and
    `UserList`. The `UserList` class stores a list in its `.data` attribute, and we can replace this with the
    "current version" of our list.

    In this example, we set a default version, so that the user does not have to create a version before using the
    versioned list. Also, we use strings for the "version type". The factory function maps versions to empty lists.

    >>> class VersionedList(SimpleVersioning[list], UserList):
    >>>     def __init__(self, iterable=None):
    >>>         super().__init__(factory=lambda x: [], versions=["v1"])  # we don't use UserList's __init__
    >>>         if iterable:
    >>>             self.extend(iterable)  # we create version "v1" initially, so we can use UserList's extend method to add initial data
    >>>
    >>>     @property
    >>>     def data(self) -> list:
    >>>         return self._current  # return current version; all of UserList's methods apply to this list

    Now we can use the versioned list as follows:

    >>> vlist = VersionedList()
    >>> vlist.append("a")
    >>> vlist
    ['a']

    >>> vlist.create_version("v2", copy_current=True, checkout=True)
    >>> vlist.append("b")
    >>> vlist
    ['a', 'b']

    >>> vlist.extend(["c", "d"])
    >>> vlist
    ['a', 'b', 'c', 'd']

    >>> vlist[0] = "z"
    >>> vlist
    ['z', 'b', 'c', 'd']

    >>> vlist.checkout_version("v1")
    >>> vlist
    ['a']

    Thus we see that we can edit version 2 without affecting version 1.
    """

    def __init__(
        self,
        factory: Callable[[str], T],
        versions: Iterable[str] | None = None,
        super_init: bool = False,
        **kwargs: Any,
    ) -> None:
        """Create SimpleVersioning object.

        Args:
            factory: factory function to create the versioned objects given a
            version string.
            versions: initial list of versions.
            super_init: if True, pass kwargs to `super().__init__`, which might
              be helpful, since this class is intended to be used with multiple
              inheritence.
            **kwargs: arguments to pass via `super().__init__`; these are ignored if
              if `super_init` is False.

        """
        self.factory = factory

        if versions is None:
            self._versions = {}
            self._current_version: str | None = None
        else:
            self._versions = {v: factory(v) for v in versions}
            self._current_version = self.versions[-1]  # version added last

        # we might want to pass arguments to
        if super_init:
            super().__init__(**kwargs)

    @property
    def versions(self) -> list[str]:
        """List of stored versions."""
        return list(self._versions.keys())

    @property
    def current_version(self) -> str:
        """Current version."""
        if self._current_version is None:
            raise VersionError("No version is selected. Use `checkout_version` to select a version.")
        return self._current_version

    @property
    def _current(self) -> T:
        return self._versions[self.current_version]

    def checkout_version(self, v: str) -> None:
        """Checkout specified version.

        Args:
            v: version to check out.

        Raises:
            ValueError: if specified version not found.

        """
        if v not in self.versions:
            raise ValueError(f"Version {v} does not exist.")
        self._current_version = v

    def copy_to_version(self, v: str) -> None:
        """Copy current version to specified version.

        The version "v" is created if it doesn't exist, and is overwritten otherwise.

        This method can be customised in subclasses if more sophisticated
        copying methods are available. (This is the primary reason for making this
        method separate from `create_version`.) Care should be taken to clean up if a
        new version is created but the copy fails.

        Args:
            v: version to copy to

        Raises:
            VersionError if no version is currently checked out.

        """
        self._versions[v] = deepcopy(self._current)

    def create_version(self, v: str, checkout: bool = False, copy_current: bool = False) -> None:
        """Create new version.

        Args:
            v: version to create.
            checkout: if True, checkout the newly created version.
            copy_current: if True, copy current version to new version.

        Raises:
            ValueError: if new version `v` already exists; if `copy_current` is
            True and no version is currently checked out.

        """
        if v in self.versions:
            raise ValueError(f"Cannot create version {v}; it already exists.")

        if copy_current:
            if self._current_version is None:
                # This check isn't necessary with default `copy_to_version`, but it's here for
                # safety if a subclass overrides `copy_to_version`.
                raise ValueError("Cannot copy current: no version is currently selected.")
            self.copy_to_version(v)
        else:
            self._versions[v] = self.factory(v)

        if checkout:
            self._current_version = v

    def delete_version(self, v: str) -> None:
        """Delete version.

        In addition to deleting the version from list of versions, if the type
        of a data stored in that version has a `delete` method, then that method
        is called on the data.

        Args:
            v: version to delete.

        Raises:
            ValueError: if specified version not found.

        """
        if v not in self.versions:
            raise ValueError(f"Version {v} does not exist.")

        data = self._versions[v]

        if isinstance(data, HasDelete):
            data.delete()

        del self._versions[v]

        if v == self._current_version:
            self._current_version = None


def next_version(v: str) -> str:
    """Get next version of v[number].

    For instance, if v = "v10", then return "v11".
    """
    n = int(v[1:])
    return f"v{n + 1}"


def prev_version(v: str, min_version: int = 1) -> str:
    """Get previous version of v[number].

    For instance, if v = "v10", then return "v9".
    Raise an error if v is the mininum version.
    """
    n = int(v[1:])
    if n == min_version:
        raise ValueError(f"Can't get previous version for minimum version v{n}.")
    return f"v{n + 1}"


class VersionedList(SimpleVersioning[list], UserList):
    """List with verisons.

    This works by subclassing SimpleVersioning and UserList, and overriding the
    `data` attribute of UserList (which holds a list) to return the current list.
    """

    def __init__(self, iterable: Iterable | None = None):
        super().__init__(factory=lambda _: [], versions=["v1"])

        if iterable:
            self.extend(iterable)

    @property
    def data(self) -> list:  # type: ignore
        return self._current


class VersionedDict(SimpleVersioning[dict], UserDict):
    """Dict with verisons.

    This works by subclassing SimpleVersioning and UserDict, and overriding the
    `data` attribute of UserDict (which holds a dict) to return the current dict.
    """

    def __init__(self, iterable: Mapping | None = None):
        super().__init__(factory=lambda _: {}, versions=["v1"])

        if iterable:
            self.update(iterable)

    @property
    def data(self) -> dict:  # type: ignore
        return self._current
