
__all__ = ["StorageCreds"]


def _get_storage_url():
    """Function to discover and return the default storage url"""
    return "http://fn.acquire-aaai.com:8080/t/storage"


def _get_storage_service(storage_url=None):
    """Function to return the storage service for the system

       Args:
            storage_url (str, default=None): Storage URL to use
       Returns:
            Service: Service object
    """
    if storage_url is None:
        storage_url = _get_storage_url()

    from Acquire.Client import Service as _Service
    service = _Service(storage_url, service_type="storage")

    if not service.is_storage_service():
        from Acquire.Client import LoginError
        raise LoginError(
            "You can only use a valid storage service to get CloudDrive info! "
            "The service at '%s' is a '%s'" %
            (storage_url, service.service_type()))

    assert(service is not None)

    return service


class StorageCreds:
    """This class holds the credentials necessary to access
       Drives and Files. The credentials will either be a logged
       in user, or a valid PAR
    """
    def __init__(self, user=None, storage_service=None,
                 service_url=None, par=None, secret=None):
        """Create these credentials either from a logged-in
           user and associated storage service (or URL),
           or from a valid PAR with associated secret
        """
        self._user = None
        self._storage_service = None
        self._par = None
        self._secret = None

        if user is not None:
            from Acquire.Client import User as _User
            if not isinstance(user, _User):
                raise TypeError("The user must be type User")

            if not user.is_logged_in():
                raise PermissionError("The user must be logged in!")

            self._user = user

            if storage_service is None:
                storage_service = _get_storage_service(service_url)

            self._storage_service = storage_service
            assert(storage_service is not None)

        elif par is not None:
            from Acquire.Client import PAR as _PAR
            if not isinstance(par, _PAR):
                raise TypeError("par must be type PAR")

            if par.expired():
                raise PermissionError(
                    "The passed PAR is either invalid or expired!")

            self._par = par
            self._secret = secret
            self._storage_service = par.service()
            assert(self._storage_service is not None)

        if self._storage_service is not None:
            from Acquire.Storage import StorageService as _StorageService
            if not isinstance(self._storage_service, _StorageService):
                raise TypeError("storage service must be type StorageService")

    def is_user(self):
        """Return whether authentication is via a logged-in user"""
        return self._user is not None

    def is_par(self):
        """Return whether authentication is via a PAR"""
        return self._par is not None

    def user(self):
        if self._user is None:
            raise PermissionError(
                "There is no user associated with these credentials!")
        return self._user

    def par(self):
        if self._par is None:
            raise PermissionError(
                "There is no PAR associated with these credentials!")

        return self._par

    def secret(self):
        # null secrets are allowed
        return self._secret

    def storage_service(self):
        if self._storage_service is None:
            raise PermissionError(
                "There is no storage service associated with these "
                "credentials!")

        return self._storage_service
