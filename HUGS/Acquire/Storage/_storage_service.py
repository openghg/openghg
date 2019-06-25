
import uuid as _uuid
from copy import copy as _copy
import os as _os

from Acquire.Service import Service as _Service

from ._errors import StorageServiceError

__all__ = ["StorageService"]


class StorageService(_Service):
    """This is a specialisation of Service for Storage Services"""
    def __init__(self, other=None):
        if isinstance(other, _Service):
            self.__dict__ = _copy(other.__dict__)

            if not self.is_storage_service():
                from Acquire.Storage import StorageServiceError
                raise StorageServiceError(
                    "Cannot construct an StorageService from "
                    "a service which is not an storage service!")

            # the storage service must define the ID for the compartment
            # in which user data will be stored
            self._storage_compartment_id = _os.getenv("STORAGE_COMPARTMENT")
        else:
            _Service.__init__(self)
            self._storage_compartment_id = None

    def storage_compartment(self):
        """Return the ID of the compartment in which user data will be
           stored. This should be a different compartment to the one used
           to store management data for the storage service"""
        if self._storage_compartment_id is None:
            from Acquire.Storage import StorageServiceError
            raise StorageServiceError(
                "The ID of the compartment for the storage account has not "
                "been set. This should have been set when the StorageService "
                "was constructed, e.g. via the STORAGE_COMPARTMENT env "
                "variable configured via Fn config")

        try:
            return self._storage_compartment_id
        except:
            pass

    def _call_local_function(self, function, args):
        """Internal function called to short-cut local 'remote'
           function calls
        """
        from storage.route import storage_functions as _storage_functions
        from admin.handler import create_handler as _create_handler
        handler = _create_handler(_storage_functions)
        return handler(function=function, args=args)
