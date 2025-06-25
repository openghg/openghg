from collections import UserDict, UserList
from collections.abc import Callable, Iterable, Hashable
from copy import deepcopy
from typing import Any, Generic, Mapping, Protocol, runtime_checkable, TypeVar
from typing_extensions import Self


@runtime_checkable
class HasCopyTo(Protocol):
    """Satisfied by classes with `copy_to` method.

    A `copy_to` method should copy the calling object to the "other" object
    passed as an argument.
    """

    def copy_to(self, other: Any) -> None: ...

    """Copy self to other.

    Args:
        other: object to copy self to.
    """


@runtime_checkable
class HasDelete(Protocol):
    """Satisfied by classes with `delete` method."""

    def delete(self) -> None: ...

    """Delete self."""


class VersionError(Exception): ...


VT = TypeVar("VT", bound=Hashable)  # version type
T = TypeVar("T")  # underlying class type


class SimpleVersioning(Generic[VT, T]):
    """Work with versions of an object.

    This can be used to create a "versioned" subclass of the class T in some circumstances, by
    using `SimpleVersioning` like a mixin.

    For instance, we could make a "versioned list" class by inheriting from `SimpleVersioning[str, list]` and
    `UserList`. The `UserList` class stores a list in its `.data` attribute, and we can replace this with the
    "current version" of our list.

    In this example, we set a default version, so that the user does not have to create a version before using the
    versioned list. Also, we use strings for the "version type". The factory function maps versions to empty lists.

    >>> class VersionedList(SimpleVersioning[str, list], UserList):
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
        factory: Callable[[VT], T],
        versions: Iterable[VT] | None = None,
        super_init: bool = False,
        **kwargs: Any,
    ) -> None:
        self.factory = factory

        if versions is None:
            self._versions = {}
            self._current_version: VT | None = None
        else:
            self._versions = {v: factory(v) for v in versions}
            self._current_version = self.versions[-1]  # version added last

        # we might want to pass arguments to
        if super_init:
            super().__init__(**kwargs)

    @property
    def versions(self) -> list[VT]:
        # TODO: should this return Iterable so e.g. we could return a tree?
        return list(self._versions.keys())

    @property
    def current_version(self) -> VT:
        if self._current_version is None:
            raise VersionError("No version is selected. Use `checkout_version` to select a version.")
        return self._current_version

    @property
    def _current(self) -> T:
        return self._versions[self.current_version]

    def _get_version(self, v: Hashable) -> VT | None:
        # use loop to check equality in case version type VT
        # can be equal to other types (e.g. string)
        for version in self.versions:
            if v == version:
                return version
        return None

    def checkout_version(self, v: Hashable) -> None:
        version = self._get_version(v)
        if version is None:
            raise ValueError(f"Version {v} does not exist.")
        self._current_version = version

    def create_version(self, v: VT, checkout: bool = False, copy_current: bool = False) -> None:
        if v in self.versions:
            raise ValueError(f"Cannot create version {v}; it already exists.")

        # need to check copying behavior first, since if T does not have `copy_to` method, we will
        # use `deepcopy`, in which case, we don't want to call the factory function, since it might
        # have side-effects (such as creating files/folders, etc.)
        if copy_current:
            try:
                current = self._current
            except VersionError as e:
                raise ValueError("Cannot copy current: no version is currently selected.") from e
            else:
                if isinstance(current, HasCopyTo):
                    self._versions[v] = self.factory(v)
                    current.copy_to(self._versions[v])
                else:
                    self._versions[v] = deepcopy(self._current)
        else:
            self._versions[v] = self.factory(v)

        if checkout:
            self._current_version = v

    def delete_version(self, v: Hashable) -> None:
        version = self._get_version(v)
        if version is None:
            raise ValueError(f"Version {v} does not exist.")

        data = self._versions[version]

        if isinstance(data, HasDelete):
            data.delete()

        del self._versions[version]

        if version == self._current_version:
            self._current_version = None


class LinearVersion:
    """Versions numbers of the form `v1`, `v2`, etc.

    LinearVersion objects can be compared with strings:

    >>> LinearVersion("v1") == "v1"
    True

    The next version number can be accessed via `.next`:

    >>> LinearVersion("v1").next == "v2"
    True

    Thus LinearVersion objects can be used like the strings "v1", "v2", ..., but
    you don't need to extract the version number to increment the version.

    Also, LinearVersion includes some validation for the version format.
    """

    def __init__(self, version: int | str) -> None:
        if isinstance(version, int):
            self.number = version
            self.version = f"v{version}"
        else:
            err_msg = f"version string {version} is not of the form 'v{{integer}}'."
            if not version.startswith("v"):
                raise ValueError(err_msg)
            try:
                self.number = int(version[1:])
            except ValueError as e:
                raise ValueError(err_msg) from e
            else:
                self.version = version

    def __str__(self) -> str:
        return self.version

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any, /) -> bool:
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, int):
            return self.number == other
        if isinstance(other, LinearVersion):
            return self.number == other.number
        raise ValueError(f"Cannot compare LinearVersion with type {type(other)}.")

    def __lt__(self, other: Any, /) -> bool:
        if isinstance(other, str):
            return str(self) < other
        if isinstance(other, int):
            return self.number < other
        if isinstance(other, LinearVersion):
            return self.number < other.number
        raise ValueError(f"Cannot compare LinearVersion with type {type(other)}.")

    @property
    def next(self) -> Self:
        """Next (child) version label."""
        return type(self)(self.number + 1)

    @property
    def prev(self) -> Self:
        """Previous (parent) version label."""
        return type(self)(self.number - 1)


class VersionedList(SimpleVersioning[LinearVersion, list], UserList):
    """List with verisons.

    This works by subclassing SimpleVersioning and UserList, and overriding the
    `data` attribute of UserList (which holds a list) to return the current list.
    """

    def __init__(self, iterable: Iterable | None = None):
        super().__init__(factory=lambda _: [], versions=[LinearVersion("v1")])

        if iterable:
            self.extend(iterable)

    @property
    def data(self) -> list:  # type: ignore
        return self._current


class VersionedDict(SimpleVersioning[LinearVersion, dict], UserDict):
    """Dict with verisons.

    This works by subclassing SimpleVersioning and UserDict, and overriding the
    `data` attribute of UserDict (which holds a dict) to return the current dict.
    """

    def __init__(self, iterable: Mapping | None = None):
        super().__init__(factory=lambda _: {}, versions=[LinearVersion("v1")])

        if iterable:
            self.update(iterable)

    @property
    def data(self) -> dict:  # type: ignore
        return self._current
