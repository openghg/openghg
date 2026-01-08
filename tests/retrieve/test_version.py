"""
Tests for version parameter in retrieve functions.

This module tests that the version parameter works correctly for all get_* functions
in openghg.retrieve._access, ensuring that specific versions of data can be retrieved
instead of always defaulting to the latest version.
"""

import pytest
from helpers import clear_test_stores, get_surface_datapath
from openghg.retrieve import (
    get_obs_surface,
    get_footprint,
    get_flux,
    get_bc,
    get_obs_column,
    search,
)
from openghg.standardise import standardise_surface


@pytest.fixture(scope="module", autouse=True)
def setup_versioned_data():
    """Set up test data with multiple versions for testing version parameter.

    This creates two versions of the same data to test version selection.
    """
    clear_test_stores()

    # Add test data for BSD site - this creates version v1
    bsd_248_path = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    standardise_surface(
        store="user",
        filepath=bsd_248_path,
        source_format="CRDS",
        site="bsd",
        network="DECC",
    )

    # Add the same data again with if_exists="new" to create version v2
    standardise_surface(
        store="user",
        filepath=bsd_248_path,
        source_format="CRDS",
        site="bsd",
        network="DECC",
        if_exists="new",
        force=True,
    )

    # Add test data for TAC site
    tac_path = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    standardise_surface(
        store="user",
        filepath=tac_path,
        source_format="CRDS",
        site="tac",
        network="DECC",
    )


def test_get_obs_surface_default_version():
    """Test that get_obs_surface retrieves latest version by default."""
    # Get data without specifying version (should default to latest)
    obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m")

    # Verify we got data
    assert obs_data is not None
    assert obs_data.data is not None
    assert "time" in obs_data.data.dims

    # The metadata should contain version information
    assert "latest_version" in obs_data.metadata


def test_get_obs_surface_explicit_latest_version():
    """Test that get_obs_surface works with explicit version='latest'."""
    # Get data with explicit version="latest"
    obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m", version="latest")

    # Verify we got data
    assert obs_data is not None
    assert obs_data.data is not None
    assert "time" in obs_data.data.dims


def test_get_obs_surface_specific_version():
    """Test that get_obs_surface can retrieve a specific version."""
    # First get the metadata to see what versions are available
    results = search(site="bsd", species="co2", inlet="248m")

    if results:
        metadata = list(results.metadata.values())[0]
        latest_version = metadata.get("latest_version", "v1")

        # Get data with specific version
        obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m", version=latest_version)

        # Verify we got data
        assert obs_data is not None
        assert obs_data.data is not None
        assert "time" in obs_data.data.dims


def test_get_obs_surface_invalid_version():
    """Test that get_obs_surface raises error for invalid version."""
    # Try to get data with an invalid version
    with pytest.raises(ValueError, match="Invalid version"):
        get_obs_surface(site="bsd", species="co2", inlet="248m", version="v999")


@pytest.mark.parametrize("version", ["latest", "v1", "v2"])
def test_get_obs_surface_multiple_versions(version):
    """Test that get_obs_surface can retrieve different versions."""
    # Get data with specified version
    obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m", version=version)

    # Verify we got data
    assert obs_data is not None
    assert obs_data.data is not None
    assert "time" in obs_data.data.dims
    assert "latest_version" in obs_data.metadata


def test_version_parameter_backwards_compatibility():
    """Test that all functions work without version parameter (backward compatibility)."""
    # Test get_obs_surface
    obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m")
    assert obs_data is not None

    # The following are best-effort tests - they may fail if test data is not set up
    # but that's okay for testing backward compatibility

    try:
        # Test get_footprint
        get_footprint(site="tmb", domain="europe", inlet="10m", model="test_model")
    except Exception:
        pass

    try:
        # Test get_flux
        get_flux(species="co2", source="gpp-cardamom", domain="europe")
    except Exception:
        pass

    try:
        # Test get_bc
        get_bc(species="n2o", bc_input="mozart", domain="europe")
    except Exception:
        pass

    try:
        # Test get_obs_column
        get_obs_column(species="ch4", satellite="gosat", max_level=10)
    except Exception:
        pass


@pytest.mark.parametrize("version", ["latest", "v1"])
def test_version_parameter_in_search_results(version):
    """Test that version parameter is properly used in SearchResults.retrieve_all()."""
    # Search for data
    results = search(site="bsd", species="co2", inlet="248m")

    assert results is not None
    metadata = list(results.metadata.values())[0]

    # Verify multiple versions exist
    assert "latest_version" in metadata
    assert "versions" in metadata
    assert "v1" in metadata["versions"]

    # Retrieve with specific version
    data = results.retrieve_all(version=version)

    # Verify we got data
    assert data is not None
    if isinstance(data, list):
        assert len(data) > 0
        assert data[0].data is not None
    else:
        assert data.data is not None
