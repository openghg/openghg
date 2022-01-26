__all__ = ["InvalidSiteError", "UnknownDataError", "FunctionError"]


class InvalidSiteError(Exception):
    """Raised if an invalid site is passed"""


class UnknownDataError(Exception):
    """Raised if an unknown data type is passed"""


class FunctionError(Exception):
    """Raised if a serverless function cannot be called correctly"""


class ObjectStoreError(Exception):
    """Raised if an error accessing an object at a key in the object store occurs"""
