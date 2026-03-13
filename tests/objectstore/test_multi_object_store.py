"""Unit tests for open_multi_object_store.

These tests create object stores directly using open_metastore (without standardise_* functions)
to test the multi-object-store search behaviour, expanded UUID formats, and routing across stores
with different data types.
"""

import pytest

from openghg.objectstore import open_multi_object_store
from openghg.objectstore.metastore import open_metastore
from openghg.types import ObjectStoreError


# ---------------------------------------------------------------------------
# Fixtures: create small in-bucket metastores for unit testing
# ---------------------------------------------------------------------------


@pytest.fixture()
def surface_store(tmp_path):
    """A single-data-type store with two surface observations."""
    bucket = str(tmp_path / "surface_store")
    with open_metastore(bucket=bucket, data_type="surface", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "surf-001",
                "site": "TAC",
                "species": "co2",
                "inlet": "185m",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )
        ms.insert(
            {
                "uuid": "surf-002",
                "site": "MHD",
                "species": "ch4",
                "inlet": "10m",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )
    return bucket


@pytest.fixture()
def flux_store(tmp_path):
    """A store containing flux data for co2."""
    bucket = str(tmp_path / "flux_store")
    with open_metastore(bucket=bucket, data_type="flux", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "flux-001",
                "species": "co2",
                "source": "gpp-cardamom",
                "domain": "europe",
                "data_type": "flux",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )
    return bucket


@pytest.fixture()
def another_surface_store(tmp_path):
    """A second surface store for testing cross-store disambiguation."""
    bucket = str(tmp_path / "surface_store_2")
    with open_metastore(bucket=bucket, data_type="surface", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "surf-alt-001",
                "site": "BSD",
                "species": "co2",
                "inlet": "42m",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )
    return bucket


# ---------------------------------------------------------------------------
# Tests: search behaviour
# ---------------------------------------------------------------------------


def test_search_all_across_two_stores(surface_store, flux_store):
    """Searching with no filters returns records from all stores."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    assert len(results) == 3


def test_search_by_species_across_different_data_types(surface_store, flux_store):
    """Species filter works across stores with different data types."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        co2_results = objstore.search(search_terms={"species": "co2"})
        ch4_results = objstore.search(search_terms={"species": "ch4"})

    # co2 appears in surface store (surf-001) and flux store (flux-001)
    assert len(co2_results) == 2
    # ch4 appears only in surface store (surf-002)
    assert len(ch4_results) == 1


def test_search_by_data_type(surface_store, flux_store):
    """Search can be filtered by data_type when merging stores."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        flux_results = objstore.search(search_terms={"data_type": "flux"})

    assert len(flux_results) == 1
    assert flux_results[0]["data_type"] == "flux"


def test_search_empty_returns_nothing(surface_store, flux_store):
    """Searching for a non-existent value returns an empty list."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search(search_terms={"site": "atlantis"})

    assert results == []


def test_multi_uuid_format_is_store__dtype__uuid(surface_store, flux_store):
    """Records carry a multi_uuid in the form 'store_name__data_type__uuid'."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    multi_uuids = {r["multi_uuid"] for r in results}
    uuids = {r["uuid"] for r in results}

    # multi_uuid must contain the original uuid as its last component
    for r in results:
        assert r["uuid"] in r["multi_uuid"]

    # all multi_uuids are distinct
    assert len(multi_uuids) == len(results)
    # original uuids are preserved
    assert uuids == {"surf-001", "surf-002", "flux-001"}


def test_object_store_name_and_data_type_added_to_records(surface_store, flux_store):
    """Each record should carry object_store_name and data_type provenance fields."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    for r in results:
        assert "object_store_name" in r, f"object_store_name missing from record {r}"
        assert "data_type" in r, f"data_type missing from record {r}"
        assert "object_store" in r, f"object_store (bucket path) missing from record {r}"


def test_object_store_name_matches_bucket_path(surface_store, flux_store):
    """object_store_name should be the bucket path used to identify the store."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    store_names = {r["object_store_name"] for r in results}
    # When explicit paths are provided, object_store_name is the full path
    assert store_names == {surface_store, flux_store}


def test_two_surface_stores_have_distinct_multi_uuids(surface_store, another_surface_store):
    """Two stores with the same data type but different records get distinct multi_uuids."""
    pairs = [(surface_store, "surface"), (another_surface_store, "surface")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    multi_uuids = [r["multi_uuid"] for r in results]
    # All multi_uuids must be unique even across the same data type
    assert len(multi_uuids) == len(set(multi_uuids)) == 3


def test_single_pair_does_not_add_multi_uuid(surface_store):
    """When only one (bucket, data_type) pair is given, multi_uuid is not added."""
    pairs = [(surface_store, "surface")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    assert len(results) == 2
    for r in results:
        assert "multi_uuid" not in r, "single-pair mode must not add multi_uuid"


def test_suppress_errors_skips_missing_data_types(surface_store, tmp_path):
    """suppress_object_store_errors=True silently skips pairs with no data."""
    empty_bucket = str(tmp_path / "empty_store")

    # flux data doesn't exist in surface_store; this would raise without suppress
    pairs = [(surface_store, "surface"), (empty_bucket, "flux")]
    with open_multi_object_store(
        bucket_data_type_pairs=pairs, suppress_object_store_errors=True
    ) as objstore:
        results = objstore.search()

    # Only the surface records from surface_store should be returned
    assert len(results) == 2


def test_new_datasource_raises_error(surface_store, flux_store):
    """Attempting to create a new datasource via open_multi_object_store raises ObjectStoreError."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        with pytest.raises(ObjectStoreError):
            objstore.create({"uuid": "new-uuid"}, data=None)


def test_search_results_contain_original_uuid(surface_store, flux_store):
    """The 'uuid' field in results must be the original datasource UUID, not the multi_uuid."""
    pairs = [(surface_store, "surface"), (flux_store, "flux")]
    with open_multi_object_store(bucket_data_type_pairs=pairs) as objstore:
        results = objstore.search()

    original_uuids = {r["uuid"] for r in results}
    assert original_uuids == {"surf-001", "surf-002", "flux-001"}
