from __future__ import annotations
from collections import UserDict
from functools import partial
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Literal, Mapping, Optional, Union

from openghg.objectstore import get_metakeys


# TODO: subclasses for different types of data + subclass registry
# TODO: should "categorising" and "optional" be separate? (e.g. added by some mixin?)
@dataclass
class Metadatum:
    """A single piece of metadata.

    It consists of a name-value pair, as well as two flags:
    - 'categorizing': if True, this Metadatum will be used for datasource look up
    - 'required': if True, then 'value' must not be None.
    """

    name: str
    value: Any
    categorising: bool = field(default=False)
    required: bool = field(default=False)

    def __post_init__(self):
        """The 'required' attribute can only be True for 'categorising' Metadatum."""
        if self.categorising is False:
            self.required = False

    @property
    def valid(self) -> bool:
        """Return False if required is True value is None."""
        invalid = self.required and (self.value is None)
        return not invalid

    def update_value(self, other: Any) -> Metadatum:
        if isinstance(other, Metadatum):
            if other.name != self.name:
                raise ValueError("Cannot update Metadatum via Metadatum with different name.")
            new_value = other.value
        else:
            new_value = other

        return replace(self, value=new_value)


class Metadata(UserDict):
    """Container for multiple Metadatum objects.

    A Metadata container behaves like a dictionary of name -> value pairs, but behind the
    scenes, the "value" is actually a Metadatum object, which allows us to control formatting
    and selecting values.

    For instance,

        >>> metadata = Metadata({"key1": Metadatum(name="key1", value="val1", categorising=True)})
        >>> metadata["key1"] == "val1"
        True

    so a Metadata object can be used as a drop-in replacement for a dictionary.

    Similarly,

        >>> list(metadata.values()) == ["val1"]
        True

    and

        >>> metadata["key1"] == "val2"
        True

    have the same effect as if `metadata` was a normal dictionary.
    """

    def __getitem__(self, key: str) -> Any:
        """Return value of Metadatum with given key.

        Note: this returns the value stored by the Metadatum object with name = key.
        It does not return the underlying Metadatum object, so that it can be used as an
        extension of a standard dict.
        """
        return super().__getitem__(key).value

    def __setitem__(self, key: str, item: Any) -> None:
        """Set value stored at `key` to `item`.

        If `key` is already stored, then the value stored at `key` is updated via the Metadatum `update_value`
        method.

        Otherwise, if `item` is Metadatum, that value is stored at `key` (provided `key` is the same as `item.name`), and
        otherwise, `item` is a stored as a Metadatum object with value = `item` and name = `key`.

        Args:
            key: key to store item under
            item: value or Metadatum object to store (or use to update an existing item)

        Returns:
            None
        """
        if key in self.data:
            metadatum = self.data[key].update_value(item)  # will raise ValueError if names differ
        elif isinstance(item, Metadatum):
            if key != item.name:
                raise ValueError(f"Cannot store Metadatum with name {item.name} under key {key}; key and name must match.")
            metadatum = item
        else:
            metadatum = Metadatum(name=key, value=item)

        super().__setitem__(key, metadatum)

    @classmethod
    def from_list(cls, _list: Iterable[Metadatum]) -> Metadata:
        """Initialise Metadata from a list (or iterable) of Metadatum objects."""
        metadata = cls()
        for x in _list:
            if not isinstance(x, Metadatum):
                raise ValueError(
                    f"All items passed to Metadata.from_list must be Metadatum objects; item {x} is {type(x)}."
                )
            metadata[x.name] = x

        return metadata

    def insert(self, item: Metadatum) -> None:
        """Insert Metadatum object into Metadata."""
        if item.name in self.data:
            raise ValueError(f"Metadatum '{item.name}' already in Metadata")
        self.data[item.name] = item

    def select(self, what: Literal["keys", "values", "items"] = "items", **conditions) -> list:
        """Get keys, values, or times filtered by given conditions.

        Conditions are given as pairs (attr, value), where 'attr' is the name of the attribute to check,
        and 'value' is the value that the attribute should match.

        If multiple conditions are passed, the items returned will satisfy all of the conditions.

        Args:
            what: what to return, either keys, values, or items.
            **conditions: conditions of the form `attribute=value`

        Returns:
            list of: (key, value) pairs if what == "items", keys if what == "keys", values if what == "values"
        """
        def _apply_condition(item: Any, attr: str, value: Any) -> bool:
            try:
                retrieved_value = item.__getattribute__(attr)
            except AttributeError:
                return False
            else:
                return retrieved_value == value

        condition_fns = [partial(_apply_condition, attr=k, value=v) for k, v in conditions.items()]

        filtered_items = [(k, v.value) for k, v in self.data.items() if all(condition(v) for condition in condition_fns)]

        if what == "items":
            return filtered_items
        elif what == "keys":
            return [k for k, _ in filtered_items]
        else:
            return [v for _, v in filtered_items]

    @staticmethod
    def merge(
        left: Metadata,
        right: Metadata,
        join: Literal["union", "intersection", "left", "right"] = "union",
        on_conflict: Literal["left", "right", "error", "drop"] = "left",
    ) -> Metadata:
        """Merge two Metadata objects together.

        Args:
            left: Metadata to merge
            right: Metadata to merge
            join: how to combine the dictionaries
                - "union": take the union of the keys (i.e. use all keys from both dicts)
                - "intersection": take the intersection of the keys (i.e. use only keys that appear in both dicts)
                - "left": use the keys from the left dictionary
                - "right": use the keys from the right dictionary
            on_conflict: behavior for differing values for the same key in both metadata dicts
                - "left": take value from left
                - "right": take value from right
                - "error": raise error
                - "drop": drop key

        Returns:
            merged Metadata
        """
        merged = merge_dicts(left.data, right.data, join=join, on_conflict=on_conflict)
        return Metadata(merged)


def merge_dicts(
    left: dict,
    right: dict,
    join: Literal["union", "intersection", "left", "right"] = "union",
    on_conflict: Literal["left", "right", "error", "drop"] = "left",
) -> dict:
    """Merge multiple metadata dictionaries together.

    Args:
        left: dictionary of metadata to merge
        right: dictionary of metadata to merge
        join: how to combine the dictionaries
            - "union": take the union of the keys (i.e. use all keys from both dicts)
            - "intersection": take the intersection of the keys (i.e. use only keys that appear in both dicts)
            - "left": use the keys from the left dictionary
            - "right": use the keys from the right dictionary
        on_conflict: behavior for differing values for the same key in both metadata dicts
            - "left": take value from left
            - "right": take value from right
            - "error": raise error
            - "drop": drop key

    Returns:
        merged dictionary
    """
    # check case: on_conflict = "error"
    overlap = set(left.keys()) & set(right.keys())
    if on_conflict == "error" and overlap:
        raise ValueError(f'Overlapping keys {overlap} and `on_conflict = "error"`.')

    if join == "union":
        if on_conflict == "left":
            result = right.copy()
            result.update(left)

        if on_conflict in ("right", "error"):
            # if on_conflict = "error" and no ValueError above, then keys are disjoint
            result = left.copy()
            result.update(right)

        if on_conflict == "drop":
            result = left.copy()

            # drop conflict
            for k in left:
                if k in right and (left[k] != right[k]):
                    del result[k]

            # add remaining keys from right
            for k in right:
                if k not in left:
                    result[k] = right[k]

    elif join == "intersection":
        if on_conflict == "left":
            result = {k: left[k] for k in overlap}

        if on_conflict == "right":
            result = {k: right[k] for k in overlap}

        if on_conflict == "drop":
            result = {}

    elif join == "left":
        result = left.copy()

        if on_conflict == "right":
            for k in left:
                if k in right:
                    result[k] = right[k]

        if on_conflict == "drop":
            for k in left:
                if k in right:
                    del result[k]

    elif join == "right":
        result = right.copy()

        if on_conflict == "left":
            for k in right:
                if k in left:
                    result[k] = left[k]

        if on_conflict == "drop":
            for k in right:
                if k in left:
                    del result[k]

    return result


def metadata_from_config(bucket: str, data_type: str) -> Metadata:
    """Make Metadata object from config file.

    Values will be filled with defaults or None.

    Args:
        bucket: name of object store
        data_type: type of data (e.g. "footprints", "surface", "flux", etc.)

    Returns:
        Metadata object contains required and optional keys from config file.
    """
    try:
        metakeys = get_metakeys(bucket=bucket)[data_type]
    except KeyError:
        raise ValueError(f"No metakeys for {data_type}, please update metakeys configuration file.")

    metadata = []

    for name, spec in metakeys["required"].items():
        # TODO: use type to look up appropriate subclass of Metadatum
        value = spec.get("default", None)
        metadata.append(Metadatum(name=name, value=value, categorising=True, required=True))

    for name, spec in metakeys.get("optional", {}).items():
        # TODO: use type to look up appropriate subclass of Metadatum
        value = spec.get("default", None)
        metadata.append(Metadatum(name=name, value=value, categorising=True, required=False))

    return Metadata.from_list(metadata)


def categorising_keys_valid(metadata: Metadata) -> bool:
    """Return True if all categorising items are valid."""
    n_cat = len(metadata.select(categorising=True))
    n_cat_valid = len(metadata.select(categorising=True, valid=True))
    return n_cat == n_cat_valid


def get_datasource_lookup_metadata(metadata: Metadata, min_keys: Optional[int] = None) -> dict[str, Any]:
    """Get metadata needed for datasource look up."""
    if not categorising_keys_valid(metadata):
        invalid_keys = metadata.select("keys", categorising=True, valid=False)
        valid_keys = metadata.select("keys", categorising=True, valid=True)

        if min_keys is None:
            invalid_keys_str = "\n\t".join(invalid_keys)
            raise ValueError(f"The following required keys are missing:{invalid_keys_str}")

        elif len(valid_keys) < min_keys:
            raise ValueError(
                f"{min_keys} required keys necessary, but only {len(valid_keys)} required keys provided."
            )

    return {k: v for k, v in metadata.select(categorising=True, valid=True)}  # TODO: add `if v is not None` ?
