
import uuid as _uuid

__all__ = ["BucketHandle"]


class BucketHandle:
    """This class represents a handle to a bucket in the object
       store. This handle can be used to generate pre-authenticated
       request URLs (PARs - also called pre-signed requests).

       Every bucket has a unique UID. It is up to you to map a
       UID to one or more meaningful names
    """
    def __init__(self, uid=None, compartment=None):
        """Construct a handle to a bucket. If the UID is None then
           this creates a new bucket with a new UID. Otherwise this
           will attempt to connect to the bucket with passed UID.

           By default this will use the same compartment as the
           service bucket. If you want to specify a different
           compartment then pass this in as 'compartment'
        """
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        bucket = _get_service_account_bucket()

        if uid is None:
            # try to create a new bucket - attempt to create it three times.
            # If it fails after 3 times then raise the error why
            errors = []

            for _attempt in range(0, 3):
                uid = _uuid.uuid4()
                try:
                    # this call fails if the bucket already exists
                    self._bucket = _ObjectStore.create_bucket(bucket, uid,
                                                              compartment)
                    self._uid = uid
                    return
                except Exception as e:
                    errors.append(e)

            raise errors[-1]
        else:
            self._bucket = _ObjectStore.get_bucket(bucket, uid, compartment,
                                                   create_if_needed=False)
            self._uid = None

    def uid(self):
        """Return the UID of this bucket"""
        return self._uid

    def get_par(self, writeable=False, valid_duration=3600):
        """Return a pre-authenticated request result (PAR) to allow access
           to this bucket. The returned PAR will be valid for 'valid_duration'
           seconds from granting the request. By default PARs are valid for
           one hour
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        return _ObjectStore.create_par(self._bucket, writeable, valid_duration)
