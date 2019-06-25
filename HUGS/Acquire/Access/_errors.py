
from Acquire.Service import ServiceError as _ServiceError

__all__ = ["AccessServiceError", "RunRequestError"]


class AccessServiceError(_ServiceError):
    pass


class RunRequestError(Exception):
    pass
