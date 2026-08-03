import warnings
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest
import xarray as xr

from openghg.standardise import (
    standardise_bc,
    standardise_column,
    standardise_eulerian,
    standardise_flux,
    standardise_flux_timeseries,
    standardise_footprint,
    standardise_site_met,
    standardise_surface,
)
from openghg.store import ObsSurface
from openghg.store.base import BaseStore

STANDARDISE_CASES = [
    pytest.param(
        standardise_surface,
        {
            "source_format": "openghg",
            "network": "test-network",
            "site": "test-site",
            "data": xr.Dataset(),
        },
        id="surface",
    ),
    pytest.param(
        standardise_column,
        {"filepath": "unused", "species": "co2"},
        id="column",
    ),
    pytest.param(
        standardise_bc,
        {
            "filepath": "unused",
            "species": "co2",
            "bc_input": "test-model",
            "domain": "test-domain",
        },
        id="bc",
    ),
    pytest.param(
        standardise_footprint,
        {"filepath": "unused", "model": "test-model", "domain": "test-domain"},
        id="footprint",
    ),
    pytest.param(
        standardise_flux,
        {
            "filepath": "unused",
            "species": "co2",
            "source": "test-source",
            "domain": "test-domain",
        },
        id="flux",
    ),
    pytest.param(
        standardise_eulerian,
        {"filepath": "unused", "model": "test-model", "species": "co2"},
        id="eulerian",
    ),
    pytest.param(
        standardise_flux_timeseries,
        {"filepath": "unused", "species": "co2", "source": "test-source"},
        id="flux-timeseries",
    ),
    pytest.param(
        standardise_site_met,
        {"filepath": "unused", "site": "test-site", "network": "test-network"},
        id="site-met",
    ),
]


def assert_force_deprecation_warning(caught_warnings: list[warnings.WarningMessage]) -> None:
    """Assert one force deprecation warning with migration guidance was emitted."""
    assert len(caught_warnings) == 1
    warning = caught_warnings[0]
    assert issubclass(warning.category, DeprecationWarning)
    message = str(warning.message).lower()
    assert "force" in message
    assert "deprecated" in message
    assert "if_exists" in message


@pytest.mark.parametrize(("standardise_fn", "kwargs"), STANDARDISE_CASES)
def test_standardise_force_true_warns(
    standardise_fn: Callable[..., list[dict]], kwargs: dict[str, Any]
) -> None:
    """Hash-era force=True warns even though the compatibility argument is ignored."""
    with (
        patch("openghg.standardise._standardise.standardise", return_value=[]),
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")
        standardise_fn(**kwargs, force=True)

    assert_force_deprecation_warning(caught_warnings)


@pytest.mark.parametrize(("standardise_fn", "kwargs"), STANDARDISE_CASES)
@pytest.mark.parametrize("force_kwargs", [{}, {"force": False}], ids=["default", "false"])
def test_standardise_force_false_does_not_warn(
    standardise_fn: Callable[..., list[dict]],
    kwargs: dict[str, Any],
    force_kwargs: dict[str, bool],
) -> None:
    """The default and explicit force=False remain warning-free."""
    with (
        patch("openghg.standardise._standardise.standardise", return_value=[]),
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")
        standardise_fn(**kwargs, **force_kwargs)

    assert caught_warnings == []


@pytest.mark.parametrize("force_kwargs", [{}, {"force": False}], ids=["default", "false"])
def test_base_store_force_false_does_not_warn(force_kwargs: dict[str, bool]) -> None:
    """BaseStore does not warn unless its ignored force compatibility flag is true."""
    store = object.__new__(BaseStore)

    with (
        patch.object(store, "format_inputs", return_value={}),
        patch.object(store, "_standardise_and_store", return_value=[]),
        patch("openghg.store.spec.check_parser", return_value="openghg"),
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")
        store.standardise_and_store(source_format="openghg", data=xr.Dataset(), **force_kwargs)

    assert caught_warnings == []


def test_base_store_force_true_warns() -> None:
    """BaseStore force=True emits deprecation and if_exists migration guidance."""
    store = object.__new__(BaseStore)

    with (
        patch.object(store, "format_inputs", return_value={}),
        patch.object(store, "_standardise_and_store", return_value=[]),
        patch("openghg.store.spec.check_parser", return_value="openghg"),
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")
        store.standardise_and_store(source_format="openghg", data=xr.Dataset(), force=True)

    assert_force_deprecation_warning(caught_warnings)


@pytest.mark.parametrize("force_kwargs", [{}, {"force": False}], ids=["default", "false"])
def test_obs_surface_store_data_force_false_does_not_warn(force_kwargs: dict[str, bool]) -> None:
    """ObsSurface.store_data stays warning-free for false force values."""
    store = object.__new__(ObsSurface)

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        store.store_data(data=[], **force_kwargs)

    assert caught_warnings == []


def test_obs_surface_store_data_force_true_warns() -> None:
    """ObsSurface.store_data force=True warns with migration guidance."""
    store = object.__new__(ObsSurface)

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        store.store_data(data=[], force=True)

    assert_force_deprecation_warning(caught_warnings)
