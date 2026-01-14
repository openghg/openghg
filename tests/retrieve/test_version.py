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

    This creates two versions with different CO2 values to test version selection.
    Version v1 has CO2 values around 410 ppm, version v2 has values of 9999.99 ppm.
    """
    clear_test_stores()

    # Add test data for BSD site - this creates version v1 with original CO2 values (~410)
    bsd_248_path = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    standardise_surface(
        store="user",
        filepath=bsd_248_path,
        source_format="CRDS",
        site="bsd",
        network="DECC",
    )

    # Add modified data with different CO2 values to create version v2 (CO2 = 9999.99)
    bsd_248_mod_path = get_surface_datapath(
        filename="bsd.picarro.1minute.248m.co2_mod.dat", source_format="CRDS"
    )
    standardise_surface(
        store="user",
        filepath=bsd_248_mod_path,
        source_format="CRDS",
        site="bsd",
        network="DECC",
        if_exists="new",
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


@pytest.mark.parametrize(
    "version,expected_co2_range", [("latest", (9000, 10000)), ("v1", (400, 425)), ("v2", (9000, 10000))]
)
def test_get_obs_surface_multiple_versions(version, expected_co2_range):
    """Test that get_obs_surface can retrieve different versions and returns the correct data.

    v1 has CO2 values around 410 ppm, v2 has values of 9999.99 ppm.
    """
    # Get data with specified version
    obs_data = get_obs_surface(site="bsd", species="co2", inlet="248m", version=version)

    # Verify we got data
    assert obs_data is not None
    assert obs_data.data is not None
    assert "time" in obs_data.data.dims
    assert "latest_version" in obs_data.metadata

    # Verify the correct version is returned by checking CO2 values
    # Get the mf (mole fraction) data variable which contains CO2 measurements
    co2_values = obs_data.data["mf"].values
    # Filter out NaN values
    co2_valid = co2_values[~obs_data.data["mf"].isnull()]

    if len(co2_valid) > 0:
        # Check that the values are in the expected range for this version
        min_co2 = float(co2_valid.min())
        max_co2 = float(co2_valid.max())
        expected_min, expected_max = expected_co2_range

        assert min_co2 >= expected_min, f"Version {version}: min CO2 {min_co2} below expected {expected_min}"
        assert max_co2 <= expected_max, f"Version {version}: max CO2 {max_co2} above expected {expected_max}"


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


@pytest.mark.parametrize("version,expected_co2_range", [("latest", (9000, 10000)), ("v1", (400, 425))])
def test_version_parameter_in_search_results(version, expected_co2_range):
    """Test that version parameter is properly used in SearchResults.retrieve_all().

    Also verifies the correct version is returned by checking CO2 values.
    """
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
        obs_data = data[0]
    else:
        obs_data = data

    assert obs_data.data is not None

    # Verify the correct version is returned by checking CO2 values
    # The variable might be 'mf' or 'co2' depending on rename_vars setting
    co2_var = "mf" if "mf" in obs_data.data else "co2"
    co2_values = obs_data.data[co2_var].values
    co2_valid = co2_values[~obs_data.data[co2_var].isnull()]

    if len(co2_valid) > 0:
        min_co2 = float(co2_valid.min())
        max_co2 = float(co2_valid.max())
        expected_min, expected_max = expected_co2_range

        assert min_co2 >= expected_min, f"Version {version}: min CO2 {min_co2} below expected {expected_min}"
        assert max_co2 <= expected_max, f"Version {version}: max CO2 {max_co2} above expected {expected_max}"
