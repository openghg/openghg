
__all__ = ["QRCodeError", "LoginError", "AccountError",
           "PaymentError", "UserError", "PARError",
           "PARTimeoutError", "PARPermissionsError",
           "PARReadError", "PARWriteError"]


class QRCodeError(Exception):
    pass


class LoginError(Exception):
    pass


class AccountError(Exception):
    pass


class UserError(Exception):
    pass


class PaymentError(Exception):
    pass


class PARError(Exception):
    pass


class PARTimeoutError(PARError):
    pass


class PARPermissionsError(PARError):
    pass


class PARReadError(PARError):
    pass


class PARWriteError(PARError):
    pass
