import tempfile

import pytest
import xarray as xr

from openghg.util import get_data, load_standardise_parser, read_header


def test_load_standardise_parser():
    f = load_standardise_parser(data_type="surface", source_format="crds")
    assert f


def test_load_standardise_parser_upper():
    f = load_standardise_parser(data_type="surface", source_format="CRDS")
    assert f


def test_load_standardise_parser_cannot_find():
    with pytest.raises(AttributeError):
        load_standardise_parser(data_type="surface", source_format="spam")


def test_read_header():
    header = "\n".join(["#", "#", "#", "#", "#"])
    dollar_header = "\n".join(["$", "$", "$", "$", "$"])

    with tempfile.NamedTemporaryFile(mode="w+t") as tmpfile:
        tmpfile.write(header)
        tmpfile.flush()

        result = read_header(filepath=tmpfile.name)
        assert len(result) == 5

    with tempfile.NamedTemporaryFile(mode="w+t") as tmpfile:
        tmpfile.write(dollar_header)
        tmpfile.flush()

        result = read_header(filepath=tmpfile.name, comment_char="$")
        assert len(result) == 5

    with tempfile.NamedTemporaryFile(mode="w+t") as tmpfile:
        tmpfile.write("sausages")
        tmpfile.flush()

        result = read_header(filepath=tmpfile.name, comment_char="$")
        assert not result


def test_get_data_direct_default_does_not_require_time():
    """Legacy direct-Dataset callers opt into coordinate validation explicitly."""
    dataset = xr.Dataset(attrs={"species": "ch4"})

    with get_data(dataset=dataset) as result:
        xr.testing.assert_identical(result, dataset)


def test_get_data_direct_explicit_coordinate_is_validated():
    """An explicit coordinate check applies to direct Dataset input."""
    with pytest.raises(ValueError, match="Expected coordinate: 'time'"):
        with get_data(dataset=xr.Dataset(), check_coords="time"):
            pass


def test_get_data_filepath_default_still_requires_time(tmp_path):
    """The historical filepath default continues to validate time coordinates."""
    filepath = tmp_path / "without_time.nc"
    xr.Dataset({"value": ("index", [1.0])}).to_netcdf(filepath)

    with pytest.raises(ValueError, match="Expected coordinate: 'time'"):
        with get_data(filepath=filepath):
            pass
