
__all__ = ["AccountError", "PackingError", "UnpackingError",
           "RemoteFunctionCallError", "ServiceError", "ServiceAccountError",
           "MissingServiceAccountError", "MissingServiceError"]


class AccountError(Exception):
    pass


class PackingError(Exception):
    pass


class UnpackingError(Exception):
    pass


class RemoteFunctionCallError(Exception):
    """This exception is called if there is a remote function
       call error that could not be auto-converted to anything
       else. If a child exception occurred remotely, then this
       is packaged safely into this exception
    """
    def __init__(self, message, child_exception=None):
        super().__init__(message)

        if child_exception is not None:
            if issubclass(child_exception.__class__, Exception):
                from ._function import exception_to_safe_exception \
                    as _exception_to_safe_exception

                self._child_exception = _exception_to_safe_exception(
                                                            child_exception)
            else:
                self.args = ("%s : %s" % (message, str(child_exception)), )

    def unpack_and_raise(self):
        if self._child_exception is None:
            raise self
        else:
            import traceback as _traceback
            tb_lines = _traceback.format_exception(
                self.__class__, self, self.__traceback__)

            self._child_exception.args = (
                "%s\n=== Local Traceback ===\n%s"
                % (self._child_exception.args[0],
                   "".join(tb_lines)), )

            if isinstance(self._child_exception, RemoteFunctionCallError):
                self._child_exception.unpack_and_raise()
            else:
                raise self._child_exception


class ServiceError(Exception):
    pass


class ServiceAccountError(Exception):
    pass


class MissingServiceAccountError(Exception):
    pass


class MissingServiceError(Exception):
    pass
