
__all__ = ["DriveInfo"]

_drive_root = "storage/drive"

_fileinfo_root = "storage/file"


def _validate_file_upload(par, file_bucket, file_key, objsize, checksum):
    """Call this function to signify that the file associated with
       this PAR has been uploaded. This will check that the
       objsize and checksum match with what was promised
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore
    from Acquire.Service import get_service_account_bucket \
        as _get_service_account_bucket
    from Acquire.Service import get_this_service as _get_this_service

    service = _get_this_service()
    bucket = _get_service_account_bucket()

    file_bucket = _ObjectStore.get_bucket(
                        bucket=bucket, bucket_name=file_bucket,
                        compartment=service.storage_compartment(),
                        create_if_needed=True)

    # check that the file uploaded matches what was promised
    (real_objsize, real_checksum) = _ObjectStore.get_size_and_checksum(
                                                file_bucket, file_key)

    if real_objsize != objsize or real_checksum != checksum:
        # probably should delete the broken object here...

        from Acquire.Storage import FileValidationError
        raise FileValidationError(
            "The file uploaded does not match what was promised. "
            "size: %s versus %s, checksum: %s versus %s. Please try "
            "to upload the file again." %
            (real_objsize, objsize,
             real_checksum, checksum))

    # SHOULD HERE RECEIPT THE STORAGE TRANSACTION


def _validate_bulk_upload(par, bucket_uid, identifiers,
                          drive_uid, aclrules, max_size):
    """Call this internal function to signify that the bulk upload
       is complete. The files have been uploaded to the bucket with
       name 'bucket_uid', and were uploaded by the user with passed
       user_guid. The files should be placed into the drive with
       specified drive_uid, using the passed ACLRules, and they
       should not have a total size greater than 'max_size'
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore
    from Acquire.Service import get_service_account_bucket \
        as _get_service_account_bucket
    from Acquire.Service import get_this_service as _get_this_service
    from Acquire.Storage import DriveInfo as _DriveInfo

    service = _get_this_service()
    bucket = _get_service_account_bucket()

    drive = _DriveInfo(drive_uid=drive_uid)

    tmpbucket = _ObjectStore.get_bucket(
                                bucket=bucket,
                                bucket_name=bucket_uid,
                                compartment=service.storage_compartment(),
                                create_if_needed=False)

    try:
        total_size = drive.copy_from(bucket=tmpbucket, aclrules=aclrules)
    except Exception as e:
        # delete the bucket and force the user to upload again...
        _ObjectStore.delete_bucket(bucket=tmpbucket, force=True)
        raise e

    if total_size > max_size:
        # should be cross with the user - give them time to make up
        # the difference in cost, or else we will delete this data
        pass

    # SHOULD NOW RECEIPT THE STORAGE TRANSACTION

    # the tmpbucket should now be empty, and all files transferred
    _ObjectStore.delete_bucket(bucket=tmpbucket, force=True)


class DriveInfo:
    """This class provides a service-side handle to the information
       about a particular cloud drive
    """
    def __init__(self, drive_uid=None, identifiers=None,
                 is_authorised=False, parent_drive_uid=None):
        """Construct a DriveInfo for the drive with UID 'drive_uid',
           and optionally the GUID of the user making the request
           (and whether this was authorised). If this drive
           has a parent then it is a sub-drive and not recorded
           in the list of top-level drives
        """
        self._drive_uid = drive_uid
        self._parent_drive_uid = parent_drive_uid
        self._identifiers = identifiers
        self._is_authorised = is_authorised

        if self._drive_uid is not None:
            self.load()

    def __str__(self):
        if self.is_null():
            return "Drive::null"
        else:
            return "Drive(%s)" % self._drive_uid

    def is_null(self):
        return self._drive_uid is None

    def _drive_key(self):
        """Return the key for this drive in the object store"""
        return "%s/%s/info" % (_drive_root, self._drive_uid)

    def uid(self):
        """Return the UID of this drive"""
        return self._drive_uid

    def _get_metadata_bucket(self):
        """Return the bucket that contains all of the metadata about
           the files for this drive
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.Service import get_this_service as _get_this_service

        service = _get_this_service()
        bucket = _get_service_account_bucket()
        bucket_name = "user_metadata"

        try:
            return _ObjectStore.get_bucket(
                            bucket=bucket, bucket_name=bucket_name,
                            compartment=service.storage_compartment(),
                            create_if_needed=True)
        except Exception as e:
            from Acquire.ObjectStore import RequestBucketError
            raise RequestBucketError(
                "Unable to open the bucket '%s': %s" % (bucket_name, str(e)))

    def _get_file_bucketname(self):
        """Return the name of the bucket that will contain all of the
           files for this drive
        """
        return "user_files"

    def _get_file_bucket(self):
        """Return the bucket that contains all of the files for this
           drive
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.Service import get_this_service as _get_this_service

        service = _get_this_service()
        bucket = _get_service_account_bucket()
        bucket_name = self._get_file_bucketname()

        try:
            return _ObjectStore.get_bucket(
                            bucket=bucket, bucket_name=bucket_name,
                            compartment=service.storage_compartment(),
                            create_if_needed=True)
        except Exception as e:
            from Acquire.ObjectStore import RequestBucketError
            raise RequestBucketError(
                "Unable to open the bucket '%s': %s" % (bucket_name, str(e)))

    def _resolve_acl(self, authorisation=None, resource=None,
                     accept_partial_match=False):
        """Internal function used to authorise access to this drive,
           returning the ACLRule of the access
        """
        if authorisation is not None:
            from Acquire.Client import Authorisation as _Authorisation
            if not isinstance(authorisation, _Authorisation):
                raise TypeError(
                    "The authorisation must be of type Authorisation")

            identifiers = authorisation.verify(
                            resource=resource,
                            accept_partial_match=accept_partial_match,
                            return_identifiers=True)
        else:
            try:
                identifiers = self._identifiers
            except:
                identifiers = None

        try:
            upstream = self._upstream
        except:
            upstream = None

        drive_acl = self.aclrules().resolve(identifiers=identifiers,
                                            upstream=upstream,
                                            must_resolve=True,
                                            unresolved=False)

        return (drive_acl, identifiers)

    def bulk_upload(self, authorisation, encrypt_key, max_size=None,
                    aclrules=None):
        """Start the process of a bulk upload of a set of files
           to this Drive. This will return a Bucket-Write PAR to
           a temporary bucket to which the files can be uploaded.
           Once the PAR is closed the files will be copied to the
           Drive and the temporary bucket deleted.

           You need to provide authorisation for this action,
           a public key to encrypt the PAR, and optionally
           specify the maximum size of the data to be uploaded
           and the ACLs. If the maximum size is not set then
           you will be limited to a maximum of 100MB upload.
           If the ACLs are not set, then they will inherit
           from the Drive (or from previous versions of the
           file if they exist)
        """
        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        from Acquire.Storage import ACLRules as _ACLRules

        if not isinstance(encrypt_key, _PublicKey):
            raise TypeError("The encryption key must be of type PublicKey")

        if aclrules is not None:
            if not isinstance(aclrules, _ACLRules):
                raise TypeError("The ACLRules must be of type ACLRules")
        else:
            aclrules = _ACLRules.inherit()

        if max_size is None:
            max_size = 100*1024*1024  # default 100 MB

        try:
            max_size = int(max_size)
        except:
            raise TypeError("max_size must be an interger!")

        (drive_acl, identifiers) = self._resolve_acl(
                        authorisation=authorisation,
                        resource="bulk_upload %s %s" % (self.uid(), max_size))

        if not drive_acl.is_writeable():
            raise PermissionError(
                "You do not have permission to write to this drive. "
                "Your permissions are %s" % str(drive_acl))

        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import Function as _Function
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.Service import get_this_service as _get_this_service

        # construct a bucket for this bulk upload, given a unique name
        bucket_uid = _create_uuid()

        func = _Function(function=_validate_bulk_upload,
                         bucket_uid=bucket_uid,
                         identifiers=identifiers,
                         drive_uid=self.uid(),
                         aclrules=aclrules.to_data(),
                         max_size=max_size)

        service = _get_this_service()
        bucket = _get_service_account_bucket()

        tmpbucket = _ObjectStore.create_bucket(
                                    bucket=bucket,
                                    bucket_name=bucket_uid,
                                    compartment=service.storage_compartment())

        try:
            par = _ObjectStore.create_par(bucket=tmpbucket,
                                          encrypt_key=encrypt_key,
                                          key=None,
                                          readable=False,
                                          writeable=True,
                                          cleanup_function=func)
        except:
            _ObjectStore.delete_bucket(bucket=tmpbucket, force=True)
            raise

        return par

    def upload(self, filehandle, authorisation, encrypt_key=None):
        """Upload the file associated with the passed filehandle.
           If the filehandle has the data embedded, then this uploads
           the file data directly and returns a FileMeta for the
           result. Otherwise, this returns a PAR which should
           be used to upload the data. The PAR will be encrypted
           using 'encrypt_key'. Remember to close the PAR once the
           file has been uploaded, so that it can be validated
           as correct
        """
        from Acquire.Client import FileHandle as _FileHandle
        from Acquire.Storage import FileInfo as _FileInfo
        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        if not isinstance(filehandle, _FileHandle):
            raise TypeError("The fileinfo must be of type FileInfo")

        if encrypt_key is not None:
            if not isinstance(encrypt_key, _PublicKey):
                raise TypeError("The encryption key must be of type PublicKey")

        (drive_acl, identifiers) = self._resolve_acl(
                        authorisation=authorisation,
                        resource="upload %s" % filehandle.fingerprint())

        if not drive_acl.is_writeable():
            raise PermissionError(
                "You do not have permission to write to this drive. "
                "Your permissions are %s" % str(drive_acl))

        # now generate a FileInfo for this FileHandle
        fileinfo = _FileInfo(drive_uid=self._drive_uid,
                             filehandle=filehandle,
                             identifiers=identifiers,
                             upstream=drive_acl)

        # resolve the ACL for the file from this FileHandle
        filemeta = fileinfo.get_filemeta()
        file_acl = filemeta.acl()

        if not file_acl.is_writeable():
            raise PermissionError(
                "Despite having write permission to the drive, you "
                "do not have write permission for the file. Your file "
                "permissions are %s" % str(file_acl))

        file_bucket = self._get_file_bucket()

        file_key = fileinfo.latest_version()._file_key()

        filedata = None

        if filehandle.is_localdata():
            # the filehandle already contains the file, so save it
            # directly
            filedata = filehandle.local_filedata()

        _ObjectStore.set_object(bucket=file_bucket,
                                key=file_key,
                                data=filedata)

        if filedata is None:
            # the file is too large to include in the filehandle so
            # we need to use a PAR to upload
            from Acquire.ObjectStore import Function as _Function

            f = _Function(function=_validate_file_upload,
                          file_bucket=self._get_file_bucketname(),
                          file_key=file_key,
                          objsize=fileinfo.filesize(),
                          checksum=fileinfo.checksum())

            par = _ObjectStore.create_par(bucket=file_bucket,
                                          encrypt_key=encrypt_key,
                                          key=file_key,
                                          readable=False,
                                          writeable=True,
                                          cleanup_function=f)
        else:
            par = None

        # now save the fileinfo to the object store
        fileinfo.save()
        filemeta = fileinfo.get_filemeta()

        assert(filemeta.acl().is_owner())

        # return the PAR if we need to have a second-stage of upload
        return (filemeta, par)

    def download(self, filename, authorisation,
                 version=None, encrypt_key=None,
                 force_par=False):
        """Download the file called filename. This will return a
           FileHandle that describes the file. If the file is
           sufficiently small, then the filedata will be embedded
           into this handle. Otherwise a PAR will be generated and
           also returned to allow the file to be downloaded
           separately. The PAR will be encrypted with 'encrypt_key'.
           Remember to close the PAR once you have finished
           downloading the file...
        """
        from Acquire.Client import FileHandle as _FileHandle
        from Acquire.Storage import FileInfo as _FileInfo
        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        if not isinstance(encrypt_key, _PublicKey):
            raise TypeError("The encryption key must be of type PublicKey")

        (drive_acl, identifiers) = self._resolve_acl(
                    authorisation=authorisation,
                    resource="download %s %s" % (self._drive_uid, filename))

        # even if the drive_acl is not readable by this user, they
        # may have read permission for the file...

        # now get the FileInfo for this FileHandle
        fileinfo = _FileInfo.load(drive=self,
                                  filename=filename,
                                  version=version,
                                  identifiers=identifiers,
                                  upstream=drive_acl)

        # resolve the ACL for the file from this FileHandle
        filemeta = fileinfo.get_filemeta()
        file_acl = filemeta.acl()

        if not file_acl.is_readable():
            raise PermissionError(
                "You do not have read permissions for the file. Your file "
                "permissions are %s" % str(file_acl))

        file_bucket = self._get_file_bucket()

        file_key = fileinfo.latest_version()._file_key()
        filedata = None
        par = None

        if force_par or fileinfo.filesize() > 1048576:
            # the file is too large to include in the download so
            # we need to use a PAR to download
            par = _ObjectStore.create_par(bucket=file_bucket,
                                          encrypt_key=encrypt_key,
                                          key=file_key,
                                          readable=True,
                                          writeable=False)
        else:
            # one-trip download of files that are less than 1 MB
            filedata = _ObjectStore.get_object(file_bucket, file_key)

        # return the filemeta, and either the filedata or par
        return (filemeta, filedata, par)

    def is_opened_by_owner(self):
        """Return whether or not this drive was opened and authorised
           by one of the drive owners
        """
        try:
            identifiers = self._identifiers
        except:
            identifiers = None

        if identifiers is None or (not self._is_authorised):
            return False

        try:
            upstream = self._upstream
        except:
            upstream = None

        return self.aclrules().resolve(identifiers=identifiers,
                                       upstream=upstream).is_owner()

    def aclrules(self):
        """Return the acl rules for this drive"""
        try:
            return self._aclrules
        except:
            from Acquire.Storage import ACLRules as _ACLRules
            return _ACLRules()

    def set_permission(self, user_guid, aclrule):
        """Set the permission for the user with the passed user_guid
           to 'aclrule". Note that you can only do this if you are the
           owner and this drive was opened in an authorised way. Also
           note that you cannot remove your own ownership permission
           if this would leave the drive without any owners
        """
        if self.is_null():
            return

        from Acquire.Identity import ACLRules as _ACLRules
        aclrules = _ACLRules.create(user_guid=user_guid, rule=aclrule)

        # make sure we have the latest version
        self.load()

        if not self.is_opened_by_owner():
            raise PermissionError(
                "You cannot change user permissions as you are either "
                "not the owner of this drive or you failed to provide "
                "authorisation when you opened the drive")

        # this will append the new rules, ensuring that the change
        # does not leave the drive ownerless
        self._aclrules.append(aclrules, ensure_owner=True)

        self.save()
        self.load()

    def list_files(self, authorisation=None, include_metadata=False):
        """Return the list of FileMeta data for the files contained
           in this Drive. The passed authorisation is needed in case
           the list contents of this drive is not public
        """
        (drive_acl, identifiers) = self._resolve_acl(
                                        authorisation=authorisation,
                                        resource="list_files")

        if not drive_acl.is_readable():
            raise PermissionError(
                "You don't have permission to read this Drive")

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import encoded_to_string as _encoded_to_string
        from Acquire.Storage import FileMeta as _FileMeta

        metadata_bucket = self._get_metadata_bucket()

        names = _ObjectStore.get_all_object_names(
                    metadata_bucket, "%s/%s" % (_fileinfo_root,
                                                self._drive_uid))

        files = []

        if include_metadata:
            # we need to load all of the metadata info for this file to
            # return to the user
            from Acquire.Storage import FileInfo as _FileInfo

            for name in names:
                data = _ObjectStore.get_object_from_json(metadata_bucket,
                                                         name)
                fileinfo = _FileInfo.from_data(data,
                                               identifiers=identifiers,
                                               upstream=drive_acl)
                filemeta = fileinfo.get_filemeta()
                file_acl = filemeta.acl()

                if file_acl.is_readable() or file_acl.is_writeable():
                    files.append(filemeta)
        else:
            for name in names:
                filename = _encoded_to_string(name.split("/")[-1])
                files.append(_FileMeta(filename=filename))

        return files

    def list_versions(self, filename, authorisation=None,
                      include_metadata=False):
        """Return the list of versions of the file with specified
           filename. If 'include_metadata' is true then this will
           load full metadata for each version. This will return
           a sorted list of FileMeta objects. The passed authorisation
           is needed in case the version info is not public
        """
        (drive_acl, identifiers) = self._resolve_acl(
                                    authorisation=authorisation,
                                    resource="list_versions %s" % filename)

        if not drive_acl.is_readable():
            raise PermissionError(
                "You don't have permission to read this Drive")

        from Acquire.Storage import FileInfo as _FileInfo
        versions = _FileInfo.list_versions(drive=self,
                                           filename=filename,
                                           identifiers=identifiers,
                                           upstream=drive_acl,
                                           include_metadata=include_metadata)

        result = []

        for version in versions:
            aclrules = version.aclrules()
            if aclrules is not None:
                acl = aclrules.resolve(upstream=drive_acl,
                                       identifiers=identifiers,
                                       must_resolve=True,
                                       unresolved=False)

                if acl.is_readable() or acl.is_writeable():
                    result.append(version)
            else:
                result.append(version)

        # return the versions sorted in upload order
        result.sort(key=lambda x: x.uploaded_when())

        return result

    def load(self):
        """Load the metadata about this drive from the object store"""
        if self.is_null():
            return

        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        bucket = _get_service_account_bucket()

        drive_key = self._drive_key()

        try:
            data = _ObjectStore.get_object_from_json(bucket, drive_key)
        except:
            data = None

        if data is None:
            # by default this user is the drive's owner
            try:
                user_guid = self._identifiers["user_guid"]
            except:
                user_guid = None

            if user_guid is None:
                # we cannot create the drive as we don't know who
                # requested it
                raise PermissionError(
                    "Cannot create the DriveInfo for a new drive as the "
                    "original request did not specify the creating user")
            elif not self._is_authorised:
                # we cannot create the drive if the request was not
                # authorised
                raise PermissionError(
                    "Cannot create the DriveInfo for a new drive as the "
                    "original request was not authorised by the user")

            # create a new drive and save it...
            from Acquire.Identity import ACLRule as _ACLRule
            from Acquire.Identity import ACLRules as _ACLRules

            self._aclrules = _ACLRules.owner(user_guid=user_guid)

            data = self.to_data()

            data = _ObjectStore.set_ins_object_from_json(bucket, drive_key,
                                                         data)

        from copy import copy as _copy
        other = DriveInfo.from_data(data)

        identifiers = self._identifiers
        is_authorised = self._is_authorised

        self.__dict__ = _copy(other.__dict__)

        self._identifiers = identifiers
        self._is_authorised = is_authorised

    def save(self):
        """Save the metadata about this drive to the object store"""
        if self.is_null():
            return

        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        bucket = _get_service_account_bucket()

        drive_key = self._drive_key()

        data = self.to_data()
        _ObjectStore.set_object_from_json(bucket, drive_key, data)

    def to_data(self):
        """Return a json-serialisable dictionary for this object"""
        data = {}

        if not self.is_null():
            from Acquire.ObjectStore import dict_to_string as _dict_to_string
            data["uid"] = self._drive_uid

            if self._aclrules is not None:
                data["aclrules"] = self._aclrules.to_data()

        return data

    @staticmethod
    def from_data(data):
        """Return a DriveInfo constructed from the passed json-deserialised
           dictionary
        """
        info = DriveInfo()

        if data is None or len(data) == 0:
            return info

        info._drive_uid = str(data["uid"])

        if "aclrules" in data:
            from Acquire.Storage import ACLRules as _ACLRules
            info._aclrules = _ACLRules.from_data(data["aclrules"])

        return info
