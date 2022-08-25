import tempfile

import pytest
from openghg.util import load_surface_parser, read_header


def test_load_surface_parser():
    f = load_surface_parser(data_type="crds")

    assert f

    f = load_surface_parser(data_type="CRDS")

    assert f

    with pytest.raises(AttributeError):
        load_surface_parser(data_type="spam")


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
