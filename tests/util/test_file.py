import tempfile

import pytest
from openghg.util import load_standardise_parser, read_header


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
