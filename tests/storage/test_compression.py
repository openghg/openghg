import numcodecs
import xarray as xr

from openghg.storage._compression import compare_compression


def test_compare_compression_writes_zip_archives(tmp_path, monkeypatch):
    """A small NetCDF input produces compression measurements from writable ZIP stores."""
    path = tmp_path / "data.nc"
    xr.Dataset({"x": ("time", [1.0, 2.0, 3.0])}).to_netcdf(path)
    monkeypatch.setattr(numcodecs.blosc, "list_compressors", lambda: ["zstd"])

    result = compare_compression(path)

    assert result["original_filesize"] == path.stat().st_size
    measurements = {key: value for key, value in result.items() if key.startswith("blosc_codec_")}
    assert len(measurements) == 9
    assert result["best_compression_codec"] in measurements
    assert result["quickest_codec"] in measurements
    for measurement in measurements.values():
        assert measurement["compressed_size_bytes"] > 0
        assert measurement["ratio"] == path.stat().st_size / measurement["compressed_size_bytes"]
        assert measurement["time_taken"] >= 0
