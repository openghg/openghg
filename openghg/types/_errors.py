from typing import Optional


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


class DatasourceCombineError(OpenGHGError):
    """Raised if we are unable to combine previous data within a Datasource with new data"""


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


class ConfigFileError(OpenGHGError):
    """Raised for errors with configuraion file"""


def construct_xesmf_import_error(exception: Optional[ImportError] = None) -> str:
    xesmf_error_message = (
        "Unable to import xesmf for use with regridding algorithms."
        " To use transform modules please follow instructions"
        " for installing non-python dependencies (requires conda"
        " to be installed even if using pip to install other packages)."
    )
    # TODO: Add explicit link to instruction page once created

    if exception:
        xesmf_error_message = f"{xesmf_error_message} Full error returned: {exception}"

    return xesmf_error_message
