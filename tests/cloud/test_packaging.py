import json

import pytest
from openghg.cloud import package_from_function, unpackage
from openghg.util import decompress, decompress_json


def test_package_from_function_metadata():
    mock_data = b"123-123-123"
    mock_metadata = {"spam": "eggs", "healthy": "snacks"}

    package = package_from_function(data=mock_data, metadata=mock_metadata)

    compressed_data = package["data"]

    file_metadata = package["file_metadata"]

    decompressed_data = decompress(data=compressed_data)
    assert decompressed_data == mock_data
    expected_sha1 = "2eb96245c157893cead0dd271dc5f2f302b1128c"

    assert file_metadata["data"]["sha1_hash"] == expected_sha1
    assert file_metadata["data"]["compression_type"] == "bz2"

    compressed_metadata = package["metadata"]
    metadata = decompress_json(data=compressed_metadata)

    assert metadata == mock_metadata

    mock_data = b"123-123-123"
    mock_metadata_str = json.dumps({"spam": "eggs", "healthy": "snacks"})

    package = package_from_function(data=mock_data, metadata=mock_metadata_str)

    compressed_metadata = package["metadata"]
    metadata = decompress_json(data=compressed_metadata)

    assert metadata == mock_metadata


def test_unpackage():
    mock_data = b"123-123-123"
    mock_metadata = {"spam": "eggs", "healthy": "snacks"}

    package = package_from_function(data=mock_data, metadata=mock_metadata)

    unpackaged = unpackage(data=package)

    assert unpackaged["data"] == mock_data
    assert unpackaged["metadata"] == mock_metadata


def test_package_sha1_mismatch():
    mock_data = b"123-123-123"
    mock_metadata = {"spam": "eggs", "healthy": "snacks"}

    package = package_from_function(data=mock_data, metadata=mock_metadata)

    package["file_metadata"]["data"]["sha1_hash"] = 888

    with pytest.raises(ValueError):
        unpackage(data=package)
