from Acquire.Service import ServiceError as _ServiceError

__all__ = ["OpenGHGServiceError"]


class OpenGHGServiceError(_ServiceError):
    pass
