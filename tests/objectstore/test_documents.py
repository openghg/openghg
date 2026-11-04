from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event

import pytest

from openghg.objectstore._documents import LocalDocuments
from openghg.objectstore._local_store import get_object_from_json, rlock, set_object_from_json


@pytest.mark.parametrize("operation", ["read", "write"])
def test_documents_coordinate_with_legacy_local_operations(tmp_path, operation):
    """A document cannot read or truncate JSON while legacy storage holds its lock."""
    documents = LocalDocuments(str(tmp_path))
    set_object_from_json(str(tmp_path), "state", {"value": "old"})
    started = Event()

    def access_document():
        started.set()
        if operation == "read":
            return documents.read("state._data")
        documents.write("state._data", {"value": "new"})
        return None

    with ThreadPoolExecutor(max_workers=1) as executor:
        with rlock:
            future = executor.submit(access_document)
            assert started.wait(timeout=5)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.1)
        result = future.result(timeout=5)

    if operation == "read":
        assert result == {"value": "old"}
    else:
        assert get_object_from_json(str(tmp_path), "state") == {"value": "new"}
