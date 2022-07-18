class InvalidSiteError(Exception):
    """Raised if an invalid site is passed"""


class UnknownDataError(Exception):
    """Raised if an unknown data type is passed"""


class FunctionError(Exception):
    """Raised if a serverless function cannot be called correctly"""


class ObjectStoreError(Exception):
    """Raised if an error accessing an object at a key in the object store occurs"""


class DatasourceLookupError(Exception):
    """Raised if Datasource lookup fails"""


class EncodingError(ObjectStoreError):
    pass


class MutexTimeoutError(Exception):
    pass


class RequestBucketError(Exception):
    pass
