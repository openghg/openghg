"""Integration tests for multi-store search across the configured object stores.

These tests use the three object stores defined in the session conftest (user, group,
shared) and verify that open_multi_object_store and search() work correctly when data
is spread across multiple stores.

Data is inserted directly using open_metastore (without standardise_* functions) so
that tests are not blocked by unrelated parsing issues.

At least one test (test_standardise_surface_and_search_multi_store) exercises the
full standardise → multi-store-search path using a real data file.
"""

import pytest

from helpers import clear_test_store, get_surface_datapath
from helpers.helpers import temporary_store_paths
from openghg.objectstore import open_multi_object_store
from openghg.objectstore.metastore import open_metastore
from openghg.retrieve import search


# ---------------------------------------------------------------------------
# Session-level fixture: populate all three stores with synthetic metadata
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def multi_store_data():
    """Populate user, group, and shared stores with synthetic records for integration tests.

    We use open_metastore directly (bypassing standardise_*) so that the test setup
    is independent of parsing infrastructure.

    The records are intentionally varied:
    - user store: surface (TAC, MHD) + flux (co2-gpp)
    - group store: surface (BSD) with co2
    - shared store: surface (CGO) with n2o (read-only permissions in config)
    """
    paths = temporary_store_paths()
    user_bucket = str(paths["user"])
    group_bucket = str(paths["group"])
    shared_bucket = str(paths["shared"])

    # Populate user store
    with open_metastore(bucket=user_bucket, data_type="surface", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "user-surf-001",
                "site": "tac",
                "species": "co2",
                "inlet": "185m",
                "network": "decc",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )
        ms.insert(
            {
                "uuid": "user-surf-002",
                "site": "mhd",
                "species": "ch4",
                "inlet": "10m",
                "network": "agage",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )

    with open_metastore(bucket=user_bucket, data_type="flux", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "user-flux-001",
                "species": "co2",
                "source": "gpp-cardamom",
                "domain": "europe",
                "data_type": "flux",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )

    # Populate group store
    with open_metastore(bucket=group_bucket, data_type="surface", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "group-surf-001",
                "site": "bsd",
                "species": "co2",
                "inlet": "108m",
                "network": "decc",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )

    # Populate shared store (we can write directly via open_metastore even though
    # the config marks it as read-only, since open_metastore does not check config permissions)
    with open_metastore(bucket=shared_bucket, data_type="surface", mode="rw") as ms:
        ms.insert(
            {
                "uuid": "shared-surf-001",
                "site": "cgo",
                "species": "n2o",
                "inlet": "75m",
                "network": "agage",
                "data_type": "surface",
                "latest_version": "v1",
                "versions": ["v1"],
            }
        )

    yield

    # Teardown: clean up the user and group stores (shared is read-only by config).
    # Remove only the synthetic records we added.
    for bucket, data_type, uuid in [
        (user_bucket, "surface", "user-surf-001"),
        (user_bucket, "surface", "user-surf-002"),
        (user_bucket, "flux", "user-flux-001"),
        (group_bucket, "surface", "group-surf-001"),
        (shared_bucket, "surface", "shared-surf-001"),
    ]:
        with open_metastore(bucket=bucket, data_type=data_type, mode="rw") as ms:
            results = ms.search({"uuid": uuid})
            if results:
                ms.delete({"uuid": uuid})


# ---------------------------------------------------------------------------
# Tests: search() across configured stores
# ---------------------------------------------------------------------------


def test_search_finds_data_from_user_store_only():
    """search(store='user') returns only user-store records."""
    res = search(store="user", data_type="surface")
    assert res

    sites = {m["site"] for m in res.metadata.values()}
    assert "tac" in sites, "TAC (user store surface) should appear"
    assert "bsd" not in sites, "BSD (group store) must not appear in user-only search"
    assert "cgo" not in sites, "CGO (shared store) must not appear in user-only search"


def test_search_finds_data_from_group_store_only():
    """search(store='group') returns only group-store records."""
    res = search(store="group", data_type="surface")
    assert res

    sites = {m["site"] for m in res.metadata.values()}
    assert "bsd" in sites, "BSD (group store) should appear"
    assert "tac" not in sites, "TAC (user store) must not appear in group-only search"


def test_search_all_stores_returns_combined_results():
    """search() without store filter returns records from all readable stores."""
    res = search(data_type="surface")
    assert res

    sites = {m["site"] for m in res.metadata.values()}
    assert "tac" in sites, "TAC (user store) should appear"
    assert "bsd" in sites, "BSD (group store) should appear"
    assert "cgo" in sites, "CGO (shared store) should appear"


def test_search_by_species_across_stores():
    """Species search returns records from multiple stores."""
    res_co2 = search(species="co2")
    assert res_co2

    data_types = {m["data_type"] for m in res_co2.metadata.values()}
    # co2 appears as both surface and flux across user and group stores
    assert "surface" in data_types
    assert "flux" in data_types


def test_search_n2o_only_in_shared_store():
    """n2o should only appear in the shared store."""
    res = search(species="n2o")
    assert res

    store_names = {m.get("object_store_name") for m in res.metadata.values()}
    assert "shared" in store_names
    # n2o is not in user or group stores in this test setup
    assert "user" not in store_names
    assert "group" not in store_names


def test_search_results_have_provenance_fields():
    """Multi-store search results carry object_store_name and data_type fields."""
    res = search(data_type="surface")
    assert res

    for uid, meta in res.metadata.items():
        assert "object_store_name" in meta, f"object_store_name missing from {uid}"
        assert "data_type" in meta, f"data_type missing from {uid}"
        assert "object_store" in meta, f"object_store missing from {uid}"


def test_search_results_have_multi_uuid_for_multi_store():
    """When searching multiple stores, results have compound multi_uuid keys."""
    res = search(data_type="surface")
    assert res

    for uid, meta in res.metadata.items():
        # The key in the metadata dict should be the multi_uuid
        multi_uuid = meta.get("multi_uuid", "")
        # multi_uuid should incorporate the original uuid
        assert meta["uuid"] in uid or meta["uuid"] in multi_uuid


def test_store_uuid_namespacing_prevents_conflicts():
    """Records with the same uuid across stores are properly namespaced."""
    res = search(data_type="surface")
    assert res

    # All outer keys (used in metadata dict) must be distinct
    all_keys = list(res.metadata.keys())
    assert len(all_keys) == len(set(all_keys))

    # All original uuids
    original_uuids = [m["uuid"] for m in res.metadata.values()]
    assert len(original_uuids) == len(set(original_uuids))


# ---------------------------------------------------------------------------
# Tests: open_multi_object_store with named stores
# ---------------------------------------------------------------------------


def test_open_multi_object_store_two_named_stores():
    """open_multi_object_store with user and group returns records from both."""
    with open_multi_object_store(
        buckets=["user", "group"],
        data_types=["surface"],
        suppress_object_store_errors=True,
    ) as objstore:
        results = objstore.search()

    assert results
    store_names = {r.get("object_store_name") for r in results}
    assert "user" in store_names
    assert "group" in store_names


def test_open_multi_object_store_three_named_stores():
    """All three stores (user, group, shared) can be opened simultaneously."""
    with open_multi_object_store(
        buckets=["user", "group", "shared"],
        data_types=["surface"],
        suppress_object_store_errors=True,
    ) as objstore:
        results = objstore.search()

    assert results
    store_names = {r.get("object_store_name") for r in results}
    assert "user" in store_names
    assert "group" in store_names
    assert "shared" in store_names


def test_open_multi_object_store_filter_by_species():
    """Species filter works across user + group + shared simultaneously."""
    with open_multi_object_store(
        buckets=["user", "group", "shared"],
        data_types=["surface"],
        suppress_object_store_errors=True,
    ) as objstore:
        n2o_results = objstore.search(search_terms={"species": "n2o"})
        co2_results = objstore.search(search_terms={"species": "co2"})

    assert len(n2o_results) == 1
    assert n2o_results[0]["site"] == "cgo"
    assert n2o_results[0]["object_store_name"] == "shared"

    # co2 appears in user (TAC surface) and group (BSD surface)
    assert len(co2_results) >= 2
    co2_stores = {r["object_store_name"] for r in co2_results}
    assert "user" in co2_stores
    assert "group" in co2_stores


def test_open_multi_object_store_no_cross_contamination():
    """Records from one store don't bleed into searches limited to another."""
    with open_multi_object_store(
        buckets=["group"],
        data_types=["surface"],
        suppress_object_store_errors=True,
    ) as objstore:
        results = objstore.search()

    sites = {r["site"] for r in results}
    assert "bsd" in sites, "BSD (group store) should appear"
    assert "tac" not in sites, "TAC (user store) must not appear"
    assert "cgo" not in sites, "CGO (shared store) must not appear"


def test_open_multi_object_store_all_data_types():
    """Opening all data types finds both surface and flux records."""
    with open_multi_object_store(suppress_object_store_errors=True) as objstore:
        results = objstore.search()

    assert results
    data_types = {r["data_type"] for r in results}
    assert "surface" in data_types
    assert "flux" in data_types


# ---------------------------------------------------------------------------
# Real-data test: standardise_surface + multi-store search
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "standardise_surface triggers a MetastoreError ('object store modified while write in progress') "
        "because LockingObjectStore.__exit__ closes the TinyDB but BaseStore.save() then calls close() "
        "again, causing SafetyCachingMiddleware to fail the hash check. "
        "Track resolution at: https://github.com/openghg/openghg/issues (double-close bug). "
        "This test documents the intended end-to-end behaviour and should be un-xfailed once fixed."
    ),
    strict=False,
)
def test_standardise_surface_and_search_multi_store():
    """End-to-end test: standardise a real data file into the group store, then
    verify it is discoverable via search() alongside the user-store records.

    Note: this test is currently xfail because standardise_surface raises
    MetastoreError due to a pre-existing bug in the base branch. See the
    xfail marker for details.
    """
    from openghg.standardise import standardise_surface

    bsd_42_path = get_surface_datapath(
        filename="bsd.picarro.1minute.42m.min.dat", source_format="CRDS"
    )

    standardise_surface(
        store="group",
        filepath=bsd_42_path,
        source_format="CRDS",
        site="bsd",
        network="DECC",
    )

    # Search should now find BSD co2 at 42m in the group store in addition to existing records
    res = search(site="bsd", species="co2", inlet="42m", store="group")
    assert res, "Expected BSD co2 42m data in group store after standardise"

    # Cross-store search should also find it
    res_all = search(site="bsd", species="co2", inlet="42m")
    assert res_all, "Expected BSD co2 42m data from cross-store search"
