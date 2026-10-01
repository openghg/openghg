"""Tests for portable xarray metadata and history attributes."""

from dataclasses import dataclass
import json
import re

import pytest
import xarray as xr

from openghg.util import (
    append_xarray_history,
    decode_xarray_metadata,
    encode_xarray_metadata,
    with_xarray_metadata,
)


@dataclass
class ExampleMetadata:
    source: str
    factors: list[float]
    options: dict[str, bool | None]


@pytest.mark.parametrize("kind", ["array", "dataset"])
@pytest.mark.parametrize("format", ["netcdf", "zarr"])
def test_metadata_and_history_round_trip(tmp_path, kind, format):
    array = xr.DataArray([1.0, 2.0], dims="time", name="value", attrs={"units": "mol"})
    original = array if kind == "array" else array.to_dataset()
    original.attrs = {"title": "example", "history": "earlier entry"}
    metadata = ExampleMetadata("test", [1.0, 2.0], {"flag": True, "missing": None})

    annotated = with_xarray_metadata(original, metadata, key="processing_metadata")
    annotated = append_xarray_history(annotated, "processed")

    assert type(annotated) is type(original)
    assert original.attrs == {"title": "example", "history": "earlier entry"}
    assert annotated.attrs["title"] == "example"
    assert decode_xarray_metadata(annotated.attrs["processing_metadata"]) == {
        "source": "test",
        "factors": [1.0, 2.0],
        "options": {"flag": True, "missing": None},
    }
    assert re.fullmatch(
        r"earlier entry\n\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+00:00: processed",
        annotated.attrs["history"],
    )

    path = tmp_path / ("result.nc" if format == "netcdf" else "result.zarr")
    if format == "netcdf":
        annotated.to_netcdf(path)
        restored = xr.open_dataarray(path) if kind == "array" else xr.open_dataset(path)
    else:
        annotated.to_zarr(path)
        stored = xr.open_zarr(path)
        restored = stored["value"] if kind == "array" else stored
    try:
        assert restored.attrs == annotated.attrs
        assert decode_xarray_metadata(restored.attrs["processing_metadata"]) == decode_xarray_metadata(
            annotated.attrs["processing_metadata"]
        )
        if kind == "dataset":
            assert restored["value"].attrs["units"] == "mol"
    finally:
        restored.close()


def test_json_schema_and_invalid_metadata():
    assert json.loads(encode_xarray_metadata({"a": [1, None]})) == {
        "schema_version": 1,
        "metadata": {"a": [1, None]},
    }
    for value in (
        "{",
        "{}",
        '{"schema_version":2,"metadata":{}}',
        '{"schema_version":true,"metadata":{}}',
        '{"schema_version":2,"schema_version":1,"metadata":{}}',
        '{"schema_version":1,"metadata":{"x":NaN}}',
    ):
        with pytest.raises(ValueError):
            decode_xarray_metadata(value)
    with pytest.raises(TypeError):
        decode_xarray_metadata(12)
    with pytest.raises(TypeError):
        encode_xarray_metadata({1: "invalid key"})
    with pytest.raises(TypeError):
        encode_xarray_metadata({"x": (1, 2)})
    with pytest.raises(ValueError):
        encode_xarray_metadata({"x": float("inf")})


def test_history_preserves_existing_newline_and_rejects_invalid_input():
    data = xr.Dataset(attrs={"history": "earlier\n", "source": "test"})
    updated = append_xarray_history(data, "later")
    assert updated.attrs["history"].startswith("earlier\n")
    assert "\n\n" not in updated.attrs["history"]
    assert updated.attrs["source"] == "test"
    assert data.attrs["history"] == "earlier\n"
    with pytest.raises(ValueError):
        append_xarray_history(data, "two\nlines")
    with pytest.raises(TypeError):
        append_xarray_history(xr.Dataset(attrs={"history": 3}), "later")
