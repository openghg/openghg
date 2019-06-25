

__all__ = ["WeakPassphraseError", "KeyManipulationError",
           "SignatureVerificationError",
           "DecryptionError", "OTPError",
           "RepeatedOTPCodeError"]


class WeakPassphraseError(Exception):
    pass


class KeyManipulationError(Exception):
    pass


class SignatureVerificationError(Exception):
    pass


class DecryptionError(Exception):
    pass


class OTPError(Exception):
    pass


class RepeatedOTPCodeError(OTPError):
    pass
