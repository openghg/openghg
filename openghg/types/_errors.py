
class OpenGHGError(Exception):
    """Top level OpenGHG error"""


class InvalidSiteError(OpenGHGError):
    """Raised if an invalid site is passed"""


class UnknownDataError(OpenGHGError):
    """Raised if an unknown data type is passed"""


class FunctionError(OpenGHGError):
    """Raised if a serverless function cannot be called correctly"""


class ObjectStoreError(OpenGHGError):
    """Raised if an error accessing an object at a key in the object store occurs"""


class DatasourceLookupError(OpenGHGError):
    """Raised if Datasource lookup fails"""


class EncodingError(ObjectStoreError):
    pass


class MutexTimeoutError(OpenGHGError):
    pass


class RequestBucketError(OpenGHGError):
    pass


class SearchError(OpenGHGError):
    """Related to searching the object store"""


class AttrMismatchError(OpenGHGError):
    """Mismatch between attributes of input file and derived metadata"""
