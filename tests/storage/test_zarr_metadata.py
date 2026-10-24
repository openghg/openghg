"""Read/write coverage for the metadata and codec conventions of each Zarr format."""

import json

from numcodecs import Blosc
import numpy as np
import pytest
import xarray as xr
import zarr

from openghg.storage import get_zarr_directory_store, get_versioned_zarr_directory_store
from openghg.storage._zarr_store import parse_to_zarr_kwargs
from openghg.storage._zarr_compat import has_async_store_api


@pytest.mark.parametrize("zarr_format", [2, 3])
@pytest.mark.parametrize("reopen_format", ["omitted", None, 2, 3])
def test_reopened_store_preserves_format_and_metadata(tmp_path, zarr_format, reopen_format):
    if zarr_format == 3 and not has_async_store_api():
        pytest.skip("Format 3 requires the Zarr 3 package")

    # Use each format's native compressor and deliberately misaligned Dask chunks.
    compressor = zarr.codecs.BloscCodec(cname="zstd", clevel=3) if zarr_format == 3 else Blosc(cname="zstd")
    original = xr.Dataset(
        {"x": ("time", np.arange(12, dtype=float))},
        coords={"time": np.arange(12)},
        attrs={"title": "metadata round trip"},
    )
    store = get_zarr_directory_store(tmp_path, zarr_format=zarr_format, compressor=compressor)
    store.insert(original.isel(time=slice(0, 6)).chunk(time=4))

    # Omitting the format or requesting another format must not convert existing data.
    kwargs = {} if reopen_format == "omitted" else {"zarr_format": reopen_format}
    reopened = get_zarr_directory_store(tmp_path, **kwargs)
    xr.testing.assert_identical(reopened.get(), original.isel(time=slice(0, 6)))
    reopened.insert(original.isel(time=slice(6, None)).chunk(time=3))
    update = original.isel(time=[1, 3, 8]).copy(deep=True)
    update["x"] += 100
    reopened.update(update.chunk(time=1))

    expected = original.copy(deep=True)
    expected["x"][dict(time=[1, 3, 8])] += 100
    xr.testing.assert_identical(get_zarr_directory_store(tmp_path).get(), expected)

    if zarr_format == 2:
        metadata = json.loads((tmp_path / ".zmetadata").read_text())["metadata"]
        assert metadata[".zgroup"]["zarr_format"] == 2
        assert metadata["x/.zarray"]["shape"] == [12]
        assert metadata["x/.zarray"]["compressor"]["id"] == "blosc"
        assert not (tmp_path / "zarr.json").exists()
    else:
        group_metadata = json.loads((tmp_path / "zarr.json").read_text())
        array_metadata = json.loads((tmp_path / "x/zarr.json").read_text())
        assert group_metadata["zarr_format"] == 3
        assert group_metadata.get("consolidated_metadata") is None
        assert array_metadata["shape"] == [12]
        assert any(codec["name"] == "blosc" for codec in array_metadata["codecs"])
        assert not (tmp_path / ".zmetadata").exists()


@pytest.mark.parametrize("requested_format", [None, 2])
def test_new_store_defaults_to_consolidated_format_two(tmp_path, requested_format):
    data = xr.Dataset({"x": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    store = get_zarr_directory_store(tmp_path, zarr_format=requested_format)
    store.insert(data)

    assert json.loads((tmp_path / ".zgroup").read_text())["zarr_format"] == 2
    assert (tmp_path / ".zmetadata").is_file()
    assert not (tmp_path / "zarr.json").exists()
    xr.testing.assert_identical(store.get(), data)


@pytest.mark.parametrize("zarr_format", [None, 2, 3])
def test_parser_accepts_supported_formats(zarr_format):
    assert parse_to_zarr_kwargs({"zarr_format": zarr_format})["zarr_format"] == zarr_format


@pytest.mark.parametrize("zarr_format", [0, 1, 4, "3"])
def test_parser_rejects_unsupported_formats(zarr_format):
    with pytest.raises(ValueError, match="zarr_format must be"):
        parse_to_zarr_kwargs({"zarr_format": zarr_format})


@pytest.mark.parametrize("operation", ["insert", "overwrite"])
def test_format_three_requires_zarr_three_before_writing(tmp_path, operation):
    if has_async_store_api():
        pytest.skip("Exercises the Zarr 2 runtime guard")
    store = get_zarr_directory_store(tmp_path, zarr_format=3)
    with pytest.raises(ValueError, match="requires the Zarr 3 package"):
        getattr(store, operation)(xr.Dataset({"x": ("time", [1.0])}, coords={"time": [0]}))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("zarr_format", [2, 3])
@pytest.mark.parametrize("reopen_format", ["omitted", None, "conflicting"])
@pytest.mark.parametrize("compressed", [False, True])
def test_overwrite_preserves_reopened_format(tmp_path, zarr_format, reopen_format, compressed):
    if zarr_format == 3 and not has_async_store_api():
        pytest.skip("Format 3 requires the Zarr 3 package")
    original = xr.Dataset({"x": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    get_zarr_directory_store(tmp_path, zarr_format=zarr_format).insert(original)
    kwargs = {}
    if reopen_format == "conflicting":
        kwargs["zarr_format"] = 3 if zarr_format == 2 else 2
    elif reopen_format is None:
        kwargs["zarr_format"] = None
    if compressed:
        kwargs["compressor"] = zarr.codecs.BloscCodec() if zarr_format == 3 else Blosc()
    reopened = get_zarr_directory_store(tmp_path, **kwargs)
    configured_kwargs = reopened.to_zarr_kwargs.copy()
    replacement = original.assign(x=original.x + 10).isel(time=slice(0, 1))

    reopened.overwrite(replacement)

    xr.testing.assert_identical(reopened.get(), replacement)
    assert (tmp_path / "zarr.json").exists() == (zarr_format == 3)
    assert (tmp_path / ".zmetadata").exists() == (zarr_format == 2)
    assert reopened.to_zarr_kwargs == configured_kwargs


@pytest.mark.parametrize("fail", [False, True])
def test_overwrite_restores_format_setting_for_new_versions(tmp_path, monkeypatch, fail):
    if not has_async_store_api():
        pytest.skip("Mixed storage formats require the Zarr 3 package")
    data = xr.Dataset({"x": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    original = get_versioned_zarr_directory_store(tmp_path, zarr_format=3)
    original.create_version("v1", checkout=True)
    original.insert(data)
    reopened = get_versioned_zarr_directory_store(tmp_path)
    configured_kwargs = reopened.to_zarr_kwargs.copy()

    if fail:

        def failed_insert(*args, **kwargs):
            raise RuntimeError("insert failed")

        with monkeypatch.context() as patch:
            patch.setattr(reopened, "insert", failed_insert)
            with pytest.raises(RuntimeError, match="insert failed"):
                reopened.overwrite(data)
    else:
        reopened.overwrite(data)
        assert (tmp_path / "v1" / "zarr.json").is_file()

    assert reopened.to_zarr_kwargs == configured_kwargs
    reopened.create_version("v2", checkout=True)
    reopened.insert(data)
    assert (tmp_path / "v2" / ".zmetadata").is_file()
