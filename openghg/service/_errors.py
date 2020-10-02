from Acquire.Service import ServiceError as _ServiceError

__all__ = ["HugsServiceError"]


class HugsServiceError(_ServiceError):
    pass
