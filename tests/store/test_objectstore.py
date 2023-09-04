from typing import TypeVar

from openghg.store.objectstore._objectstore import SupportsBucketUUIDLoad


T = TypeVar('T', bound='DumbDatasource')


class DumbDatasource:
    """Minimal class satisfying BucketUUIDLoadable protocol."""
    def __init__(self, uuid: str) -> None:
        self.uuid = uuid

    @classmethod
    def load(cls: type[T], bucket: str, uuid: str) -> T:
        return cls(uuid)


def test_dumb_datasource_is_loadable():
    """Check that DumbDatasource satisfies BucketUUIDLoadable protocol."""
    assert issubclass(DumbDatasource, SupportsBucketUUIDLoad)
