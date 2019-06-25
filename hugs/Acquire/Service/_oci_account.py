
import os as _os
import uuid as _uuid

__all__ = ["OCIAccount"]


class OCIAccount:
    """This class abstracts all interaction with OCI login accounts. This
       is a low-level account that allows us to connect to the object
       store at a low-level and to call OCI functions
    """

    @staticmethod
    def _assert_valid_login_dict(login):
        """This function validates that the passed login dictionary
           contains all of the keys needed to login"""

        if login is None:
            from Acquire.Service import AccountError
            raise AccountError("You need to supply login credentials!")

        if not isinstance(login, dict):
            from Acquire.Service import AccountError
            raise AccountError(
                "You need to supply a valid login credential dictionary!")

        needed_keys = ["user", "key_lines", "fingerprint",
                       "tenancy", "region", "pass_phrase"]

        missing_keys = []

        for key in needed_keys:
            if key not in login:
                missing_keys.append(key)

        if len(missing_keys) > 0:
            from Acquire.Service import AccountError
            raise AccountError(
                "Cannot log in as the login dictionary "
                "is missing the following data: %s" % str(missing_keys))

    @staticmethod
    def get_login(login):
        """This function turns the passed login details into
           a valid oci login
        """

        # validate that all of the information is held in the
        # 'login' dictionary
        OCIAccount._assert_valid_login_dict(login)

        # first, we need to convert the 'login' so that it puts
        # the private key into a file
        keyfile = "/tmp/key.pem"

        try:
            with open(keyfile, "w") as FILE:
                for line in login["key_lines"]:
                    FILE.write(line)

            _os.chmod(keyfile, 0o0600)

            del login["key_lines"]
            login["key_file"] = keyfile
        except:
            _os.remove(keyfile)
            raise

        return login

    @staticmethod
    def _sanitise_bucket_name(bucket_name):
        """This function sanitises the passed bucket name. It will always
           return a valid bucket name. If "None" is passed, then a new,
           unique bucket name will be generated"""

        if bucket_name is None:
            return str(_uuid.uuid4())

        return "_".join(bucket_name.split())

    @staticmethod
    def create_and_connect_to_bucket(login_details, compartment,
                                     bucket_name=None):
        """Connect to the object store compartment 'compartment'
           using the passed 'login_details', and create a bucket
           called 'bucket_name". Return a handle to the
           created bucket. If the bucket already exists this will return
           a handle to the existing bucket
        """

        try:
            from oci.object_storage import ObjectStorageClient as \
                _ObjectStorageClient
            from oci.object_storage.models import CreateBucketDetails as \
                _CreateBucketDetails
        except:
            raise ImportError(
                "Cannot import OCI. Please install OCI, e.g. via "
                "'pip install oci' so that you can connect to the "
                "Oracle Cloud Infrastructure")

        login = OCIAccount.get_login(login_details)
        bucket = {}
        client = None

        try:
            client = _ObjectStorageClient(login)

            bucket["client"] = client
            bucket["compartment_id"] = compartment

            # save the region as this is needed for some services
            bucket["region"] = login["region"]

            namespace = client.get_namespace().data
            bucket["namespace"] = namespace
            bucket["bucket_name"] = bucket_name

            try:
                request = _CreateBucketDetails()
                request.compartment_id = compartment
                request.name = OCIAccount._sanitise_bucket_name(bucket_name)

                bucket["bucket"] = client.create_bucket(
                                            client.get_namespace().data,
                                            request).data
            except Exception as e1:
                # couldn't create the bucket - likely because it already
                # exists - try to connect to the existing bucket
                try:
                    bucket["bucket"] = client.get_bucket(namespace,
                                                         bucket_name).data
                except Exception as e:
                    from Acquire.Service import AccountError
                    raise AccountError(
                        "Cannot access the bucket '%s' : %s (originally %s)" %
                        (bucket_name, str(e), str(e1)))
        except:
            _os.remove(_os.path.abspath(login["key_file"]))
            raise

        _os.remove(_os.path.abspath(login["key_file"]))

        return bucket

    @staticmethod
    def connect_to_bucket(login_details, compartment, bucket_name):
        """Connect to the object store compartment 'compartment'
           using the passed 'login_details', returning a handle to the
           bucket associated with 'bucket
        '"""

        try:
            from oci.object_storage import ObjectStorageClient as \
                _ObjectStorageClient
        except:
            raise ImportError(
                "Cannot import OCI. Please install OCI, e.g. via "
                "'pip install oci' so that you can connect to the "
                "Oracle Cloud Infrastructure")

        login = OCIAccount.get_login(login_details)
        bucket = {}

        try:
            client = _ObjectStorageClient(login)
            bucket["client"] = client
            bucket["compartment_id"] = compartment

            namespace = client.get_namespace().data
            bucket["namespace"] = namespace

            bucket["bucket"] = client.get_bucket(namespace, bucket_name).data
            bucket["bucket_name"] = bucket_name
        except:
            _os.remove(_os.path.abspath(login["key_file"]))
            raise

        _os.remove(_os.path.abspath(login["key_file"]))

        return bucket
