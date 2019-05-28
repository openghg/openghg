
__all__ = ["PARRegistry"]

_registry_key = "registry/pars"


class PARRegistry:
    """This is a PAR registry that is used
       to maintain a registry of PARs that have been created
       on an ObjectStore.
    """
    @staticmethod
    def register(par, url_checksum, details_function, cleanup_function=None):
        """Register the passed PAR, passing in the checksum of
           the PAR's secret URL (so we can verify the close),
           and optionally supplying a cleanup_function that is
           called when the PAR is closed. The passed 'details_function'
           should be used to extract the object-store driver-specific
           details from the PAR and convert them into a dictionary.
           The signature should be;

           driver_details = details_function(par)
        """
        from Acquire.Service import is_running_service as _is_running_service

        if not _is_running_service():
            return

        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        from Acquire.Client import PAR as _PAR

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import Function as _Function
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string

        if par is None:
            return

        if not isinstance(par, _PAR):
            raise TypeError("You can only register pars of type PAR")

        if par.is_null():
            return

        data = {}
        data["par"] = par.to_data()

        if details_function is None:
            data["driver_details"] = par._driver_details
        else:
            data["driver_details"] = details_function(par)

        data["url_checksum"] = url_checksum

        if cleanup_function is not None:
            if not isinstance(cleanup_function, _Function):
                cleanup_function = _Function(cleanup_function)

            data["cleanup_function"] = cleanup_function.to_data()

        expire_string = _datetime_to_string(par.expires_when())

        key = "%s/uid/%s/%s" % (_registry_key, par.uid(), expire_string)

        bucket = _get_service_account_bucket()
        _ObjectStore.set_object_from_json(bucket, key, data)

        key = "%s/expire/%s/%s" % (_registry_key, expire_string, par.uid())
        _ObjectStore.set_object_from_json(bucket, key, par.uid())

    @staticmethod
    def get(par_uid, details_function, url_checksum=None):
        """Return the PAR that matches the passed PAR_UID.
           If 'url_checksum' is supplied then this verifies that
           the checksum of the secret URL is correct.

           This returns the PAR with a completed 'driver_details'.
           The 'driver_details' is created from the dictionary
           of data saved with the PAR. The signature should be;

           driver_details = details_function(data)
        """
        if par_uid is None or len(par_uid) == 0:
            return

        from Acquire.Service import is_running_service as _is_running_service

        if not _is_running_service():
            return

        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        from Acquire.Client import PAR as _PAR

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime

        key = "%s/uid/%s" % (_registry_key, par_uid)

        bucket = _get_service_account_bucket()

        objs = _ObjectStore.get_all_objects_from_json(bucket=bucket,
                                                      prefix=key)

        data = None

        for obj in objs.values():
            if url_checksum is not None:
                if url_checksum == obj["url_checksum"]:
                    data = obj
                    break
            else:
                data = obj
                break

        if data is None:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "There is matching PAR available to close...")

        par = _PAR.from_data(data["par"])

        if "driver_details" in data:
            if details_function is not None:
                driver_details = details_function(data["driver_details"])
                par._driver_details = driver_details
            else:
                par._driver_details = driver_details

        return par

    @staticmethod
    def close(par):
        """Close the passed PAR. This will remove the registration
           for the PAR and will also call the associated
           cleanup_function (if any)
        """
        from Acquire.Service import is_running_service as _is_running_service

        if not _is_running_service():
            return

        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        from Acquire.Client import PAR as _PAR

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import Function as _Function

        if par is None:
            return

        if not isinstance(par, _PAR):
            raise TypeError("You can only close PAR objects!")

        if par.is_null():
            return

        expire_string = _datetime_to_string(par.expires_when())

        bucket = _get_service_account_bucket()

        key = "%s/expire/%s/%s" % (_registry_key, expire_string, par.uid())
        try:
            _ObjectStore.delete_object(bucket=bucket, key=key)
        except:
            pass

        key = "%s/uid/%s/%s" % (_registry_key, par.uid(), expire_string)

        try:
            data = _ObjectStore.take_object_from_json(bucket=bucket, key=key)
        except:
            data = None

        if data is None:
            # this PAR has already been closed
            return

        if "cleanup_function" in data:
            cleanup_function = _Function.from_data(data["cleanup_function"])
            cleanup_function(par=par)
