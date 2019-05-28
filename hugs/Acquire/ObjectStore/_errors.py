

__all__ = ["ObjectStoreError", "MutexTimeoutError", "EncodingError"]


class ObjectStoreError(Exception):
    pass


class EncodingError(ObjectStoreError):
    pass


class MutexTimeoutError(Exception):
    pass
