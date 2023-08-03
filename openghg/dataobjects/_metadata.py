"""
Module for defining Metadata objects and
Metadata containers.
"""
from typing import Any, Optional, Union


class Metadata:
    """Represents a piece of metadata, along
    with internal tags.
    """

    def __init__(
        self,
        key: str,
        value: Any,
        origin: Optional[str] = None,
    ) -> None:
        """Instantiate Metadata object.

        Args:
            key: analogous to dictionary key; case insensitive.
            value: analogous to dictionary value
            origin: where the metadata originates from, for instance: user input or data attributes.

        Returns:
            None
        """
        self.key = key.lower()
        self.value = value
        self.origin = origin  # TODO: should be ENUM?


class MetadataContainer:
    """Container for Metadata objects.

    These containers can be used in place of dictionaries
    for passing metadata.
    """
    def __init__(self) -> None:
        self._metadata: dict[str, Metadata] = {}

    @property
    def metadata(self) -> dict[str, Any]:
        return {md.key: md.value for md in self._metadata.values()}

    def get(self, key: str) -> Any:
        return self._metadata[key].value

    def add(self, key: str, value: Any, origin: Optional[str] = None) -> None:
        """Add metadata to the container.

        Args:
            key: key for metadata; case insensitive.
            value: metadata value to store.
            require: True if this metadata is required for Datasource lookup.
            origin: where the metadata originates from, for instance: user input or data attributes.

        Returns:
            None

        Raises:
            ValueError if `key` already exists. Use `update` method to modify existing metadata.
        """
        if key not in self._metadata.keys():
            # TODO should we allow adding None values?
            self._metadata[key] = Metadata(key, value, origin)
        else:
            raise ValueError(f"Key {key} already exists. Use `update` to modify existing metadata.")

    def update(self, key: str, value: Any, origin: Optional[str] = None) -> None:
        """Update metadata with given key.

        Args:
            key: key for metadata; case insensitive.
            value: metadata value to store.
            require: True if this metadata is required for Datasource lookup.
            origin: where the metadata originates from, for instance: user input or data attributes.

        Returns:
            None

        Raises:
            KeyError if `key` is not in the container.
        """
        self._metadata[key] = Metadata(key, value, origin)
