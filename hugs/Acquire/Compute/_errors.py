
from Acquire.Service import ServiceError as _ServiceError

__all__ = ["ComputeServiceError"]


class ComputeServiceError(_ServiceError):
    pass
