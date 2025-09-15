import pytest

from openghg.util._versioning import next_version, VersionedList


def test_versioned_list_list_operations():
    """Test basic list operations on versioned list with one version."""
    vlist = VersionedList()
    vlist.append("a")

    assert vlist == ["a"]

    vlist.append("b")

    assert vlist == ["a", "b"]

    vlist.extend(["c", "d"])

    assert vlist == ["a", "b", "c", "d"]

    vlist[0] = "z"

    assert vlist == ["z", "b", "c", "d"]


def test_versioned_list_two_versions():
    """Test that operations on a second version don't affect the first version."""
    vlist = VersionedList()
    vlist.append("a")

    assert vlist == ["a"]

    new_version = next_version(vlist.current_version)
    vlist.create_version(new_version, checkout=True, copy_current=True)

    assert vlist == ["a"]

    vlist.append("b")

    assert vlist == ["a", "b"]

    vlist.extend(["c", "d"])

    assert vlist == ["a", "b", "c", "d"]

    vlist[0] = "z"

    assert vlist == ["z", "b", "c", "d"]

    vlist.checkout_version("v1")

    assert vlist == ["a"]
