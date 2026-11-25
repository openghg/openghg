"""Tests for validation performed by the public transform interfaces."""

from pathlib import Path

import pytest
import xarray as xr

from openghg.store import BoundaryConditions, Flux
from openghg.transform import transform_bc_data, transform_flux_data


@pytest.mark.parametrize(
    ("transform", "database"),
    [(transform_flux_data, "EDGAR"), (transform_bc_data, "CAMS")],
)
@pytest.mark.parametrize("inputs", [{}, {"datapath": Path("raw.nc"), "data": xr.Dataset()}])
def test_transform_wrapper_validates_exactly_one_input_before_bucket(
    monkeypatch, transform, database, inputs
):
    """Invalid input selection must not acquire a writable object-store bucket."""
    monkeypatch.setattr(
        "openghg.transform._transform.get_writable_bucket",
        lambda **kwargs: pytest.fail("bucket acquisition must happen after validation"),
    )

    with pytest.raises(ValueError, match="exactly one"):
        transform(database=database, **inputs)


@pytest.mark.parametrize("transform", [transform_flux_data, transform_bc_data])
def test_transform_wrapper_validates_database_before_bucket(monkeypatch, transform):
    """An unsupported database must fail before a writable bucket is acquired."""
    monkeypatch.setattr(
        "openghg.transform._transform.get_writable_bucket",
        lambda **kwargs: pytest.fail("bucket acquisition must happen after validation"),
    )

    with pytest.raises(ValueError, match="Unable to transform"):
        transform(datapath=Path("raw.nc"), database="unsupported")


@pytest.mark.parametrize(
    ("store_class", "database"),
    [(Flux, "EDGAR"), (BoundaryConditions, "CAMS")],
)
def test_transform_store_defensively_validates_input_selection(store_class, database):
    """Store methods must reject ambiguous input even when called without a wrapper."""
    store = object.__new__(store_class)

    with pytest.raises(ValueError, match="exactly one"):
        store.transform_data(datapath=Path("raw.nc"), data=xr.Dataset(), database=database)


@pytest.mark.parametrize("store_class", [Flux, BoundaryConditions])
def test_transform_store_defensively_validates_database(store_class):
    """Store methods must reject missing databases before processing input."""
    store = object.__new__(store_class)

    with pytest.raises(ValueError, match="Unable to transform"):
        store.transform_data(datapath=Path("raw.nc"), database=None)
