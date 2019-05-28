
from Acquire.Service import ServiceError

__all__ = ["IdentityServiceError", "LoginSessionError", "UsernameError",
           "ExistingAccountError", "UserValidationError",
           "AuthorisationError"]


class IdentityServiceError(ServiceError):
    pass


class LoginSessionError(Exception):
    pass


class UsernameError(Exception):
    pass


class ExistingAccountError(Exception):
    pass


class UserValidationError(Exception):
    pass


class AuthorisationError(Exception):
    pass
