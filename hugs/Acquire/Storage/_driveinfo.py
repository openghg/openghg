
__all__ = ["DriveInfo"]

_drive_root = "storage/drive"

_fileinfo_root = "storage/file"

_uploader_root = "storage/uploader"
_downloader_root = "storage/downloader"


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


class DriveInfo:
    """This class provides a service-side handle to the information
       about a particular cloud drive
    """
    def __init__(self, drive_uid=None, identifiers=None,
                 is_authorised=False, parent_drive_uid=None,
                 autocreate=False):
        """Construct a DriveInfo for the drive with UID 'drive_uid',
           and optionally the GUID of the user making the request
           (and whether this was authorised). If this drive
           has a parent then it is a sub-drive and not recorded
           in the list of top-level drives

           If 'aclrule' is passed, then this drive can only be
           opened with a maximum of the permissions in the passed
           aclrule
        """
        self._drive_uid = drive_uid
        self._parent_drive_uid = parent_drive_uid
        self._identifiers = identifiers
        self._is_authorised = is_authorised

        if self._drive_uid is not None:
            self.load(autocreate=autocreate)

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

    def _get_file_bucketname(self, filekey=None):
        """Return the name of the bucket that will contain all of the
           files for this drive.

           _filekey is passed in as a stub, for a future when we will
           need to split a drive over multiple object store buckets...
        """
        return "user_files"

    def _get_file_bucket(self, filekey=None):
        """Return the bucket that contains the file data for the
           file associated with 'filekey' in this drive
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.Service import get_this_service as _get_this_service

        service = _get_this_service()
        bucket = _get_service_account_bucket()
        bucket_name = self._get_file_bucketname(filekey=filekey)

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
                     accept_partial_match=False, par=None,
                     identifiers=None):
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
        elif par is not None:
            from Acquire.Client import PAR as _PAR
            if not isinstance(par, _PAR):
                raise TypeError("The par must be type PAR")

            if par.expired():
                raise PermissionError(
                    "Cannot access the drive at the PAR has expired!")

            if identifiers is None:
                raise PermissionError(
                    "The identifiers for the user who created the PAR "
                    "must be passed!")

            if par.location().drive_uid() != self._drive_uid:
                raise PermissionError(
                    "Cannot access the drive as the PAR is not "
                    "validated for this drive!")
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

        if par is not None:
            drive_acl = drive_acl * par.aclrule()

        return (drive_acl, identifiers)

    def open_uploader(self, filename, aclrules=None, authorisation=None,
                      par=None, identifiers=None):
        """Create a return a ChunkUploader that will allow a file
           to be uploaded chunk-by-chunk (bit-by-bit). The filename
           of the files is passed as 'filename', and the aclrules
           specific for the file can also be set (otherwise will
           inherit from the drive). The authorisation, par and
           identifiers can be used to authenticate this request
        """
        (drive_acl, identifiers) = self._resolve_acl(
                        authorisation=authorisation,
                        resource="chunk_upload %s" % filename,
                        par=par, identifiers=identifiers)

        if not drive_acl.is_writeable():
            raise PermissionError(
                "You do not have permission to write to this drive. "
                "Your permissions are %s" % str(drive_acl))

        # now generate a FileInfo for this file
        from Acquire.Storage import FileInfo as _FileInfo
        fileinfo = _FileInfo(drive_uid=self._drive_uid,
                             filename=filename,
                             identifiers=identifiers,
                             upstream=drive_acl,
                             aclrules=aclrules,
                             is_chunked=True)

        # resolve the ACL for the file from this FileHandle
        filemeta = fileinfo.get_filemeta()
        file_acl = filemeta.acl()

        if not file_acl.is_writeable():
            raise PermissionError(
                "Despite having write permission to the drive, you "
                "do not have write permission for the file. Your file "
                "permissions are %s" % str(file_acl))

        from Acquire.Client import ChunkUploader as _ChunkUploader
        uploader = _ChunkUploader(drive_uid=self._drive_uid,
                                  file_uid=filemeta.uid())

        fileinfo.save()

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = "%s/%s/%s" % (_uploader_root, self._drive_uid, filemeta.uid())

        data = {"filename": filename,
                "version": filemeta.uid(),
                "filekey": fileinfo.latest_version()._file_key(),
                "secret": uploader.secret()}

        _ObjectStore.set_object_from_json(bucket, key, data)

        return (filemeta, uploader)

    def close_uploader(self, file_uid, secret):
        """Close the uploader associated with the passed file_uid,
           authenticated using the passed secret
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = "%s/%s/%s" % (_uploader_root, self._drive_uid, file_uid)

        try:
            data = _ObjectStore.get_object_from_json(bucket, key)
        except:
            data = None

        if data is None:
            # the uploader has already been closed
            return

        shared_secret = data["secret"]

        if secret != shared_secret:
            raise PermissionError(
                "Invalid request - you do not have permission to "
                "close this uploader")

        try:
            data2 = _ObjectStore.take_object_from_json(bucket, key)
        except:
            data2 = None

        if data2 is None:
            # someone else is already in the process of closing
            # this uploader - let them do it!
            return

        filename = data["filename"]
        version = data["version"]

        # now get the FileInfo for this file
        from Acquire.Storage import FileInfo as _FileInfo
        fileinfo = _FileInfo.load(drive=self,
                                  filename=filename,
                                  version=version)

        file_key = data["filekey"]
        file_bucket = self._get_file_bucket(file_key)
        fileinfo.close_uploader(file_bucket=file_bucket)
        fileinfo.save()

    def close_downloader(self, downloader_uid, file_uid, secret):
        """Close the downloader associated with the passed
           downloader_uid and file_uid,
           authenticated using the passed secret
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = "%s/%s/%s/%s" % (_downloader_root, self._drive_uid,
                               file_uid, downloader_uid)

        try:
            data = _ObjectStore.get_object_from_json(bucket, key)
        except:
            data = None

        if data is None:
            # the downloader has already been closed
            return

        shared_secret = data["secret"]

        if secret != shared_secret:
            raise PermissionError(
                "Invalid request - you do not have permission to "
                "close this downloader")

        try:
            _ObjectStore.take_object_from_json(bucket, key)
        except:
            pass

    def upload_chunk(self, file_uid, chunk_index, secret, chunk, checksum):
        """Upload a chunk of the file with UID 'file_uid'. This is the
           chunk at index 'chunk_idx', which is set equal to 'chunk'
           (validated with 'checksum'). The passed secret is used to
           authenticate this upload. The secret should be the
           multi_md5 has of the shared secret with the concatenated
           drive_uid, file_uid and chunk_index
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = "%s/%s/%s" % (_uploader_root, self._drive_uid, file_uid)
        data = _ObjectStore.get_object_from_json(bucket, key)
        shared_secret = data["secret"]

        from Acquire.Crypto import Hash as _Hash
        shared_secret = _Hash.multi_md5(shared_secret,
                                        "%s%s%d" % (self._drive_uid,
                                                    file_uid,
                                                    chunk_index))

        if secret != shared_secret:
            raise PermissionError(
                "Invalid chunked upload secret. You do not have permission "
                "to upload chunks to this file!")

        # validate the data checksum
        check = _Hash.md5(chunk)

        if check != checksum:
            from Acquire.Storage import FileValidationError
            raise FileValidationError(
                "Invalid checksum for chunk: %s versus %s" %
                (check, checksum))

        meta = {"filesize": len(chunk),
                "checksum": checksum,
                "compression": "bz2"}

        file_key = data["filekey"]
        chunk_index = int(chunk_index)

        file_bucket = self._get_file_bucket(file_key)
        data_key = "%s/data/%d" % (file_key, chunk_index)
        meta_key = "%s/meta/%d" % (file_key, chunk_index)

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        _ObjectStore.set_object_from_json(file_bucket, meta_key, meta)
        _ObjectStore.set_object(file_bucket, data_key, chunk)

    def download_chunk(self, file_uid, downloader_uid, chunk_index, secret):
        """Download a chunk of the file with UID 'file_uid' at chunk
           index 'chunk_index'. This request is authenticated with
           the passed secret. The secret should be the
           multi_md5 has of the shared secret with the concatenated
           drive_uid, file_uid and chunk_index
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = "%s/%s/%s/%s" % (_downloader_root, self._drive_uid,
                               file_uid, downloader_uid)

        try:
            data = _ObjectStore.get_object_from_json(bucket, key)
        except:
            data = None

        if data is None:
            raise PermissionError(
                "There is no downloader available to let you download "
                "this chunked file!")

        shared_secret = data["secret"]

        from Acquire.Crypto import Hash as _Hash
        shared_secret = _Hash.multi_md5(shared_secret,
                                        "%s%s%d" % (self._drive_uid,
                                                    file_uid,
                                                    chunk_index))

        if secret != shared_secret:
            raise PermissionError(
                "Invalid chunked upload secret. You do not have permission "
                "to upload chunks to this file!")

        file_key = data["filekey"]
        chunk_index = int(chunk_index)

        file_bucket = self._get_file_bucket(file_key)
        data_key = "%s/data/%d" % (file_key, chunk_index)
        meta_key = "%s/meta/%d" % (file_key, chunk_index)

        num_chunks = None

        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        try:
            meta = _ObjectStore.get_object_from_json(file_bucket, meta_key)
        except:
            meta = None

        if meta is None:
            # invalid read - see if the file has been closed?
            filename = data["filename"]
            version = data["version"]

            from Acquire.Storage import FileInfo as _FileInfo
            fileinfo = _FileInfo.load(drive=self,
                                      filename=filename,
                                      version=version)

            if fileinfo.version().is_uploading():
                raise IndexError("Invalid chunk index")

            num_chunks = fileinfo.version().num_chunks()

            if chunk_index < 0:
                chunk_index = num_chunks + chunk_index

            if chunk_index < 0 or chunk_index > num_chunks:
                raise IndexError("Invalid chunk index")
            elif chunk_index == num_chunks:
                # signal we've reached the end of the file
                return (None, None, num_chunks)

            # we should be able to read this metadata...
            meta = _ObjectStore.get_object_from_json(file_bucket, meta_key)

        chunk = _ObjectStore.get_object(file_bucket, data_key)

        return (chunk, meta, num_chunks)

    def upload(self, filehandle, authorisation=None, encrypt_key=None,
               par=None, identifiers=None):
        """Upload the file associated with the passed filehandle.
           If the filehandle has the data embedded, then this uploads
           the file data directly and returns a FileMeta for the
           result. Otherwise, this returns a PAR which should
           be used to upload the data. The PAR will be encrypted
           using 'encrypt_key'. Remember to close the PAR once the
           file has been uploaded, so that it can be validated
           as correct
        """
        from Acquire.Storage import FileHandle as _FileHandle
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
                        resource="upload %s" % filehandle.fingerprint(),
                        par=par, identifiers=identifiers)

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

        file_key = fileinfo.latest_version()._file_key()
        file_bucket = self._get_file_bucket(file_key)

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
            # we need to use a OSPar to upload
            from Acquire.ObjectStore import Function as _Function

            f = _Function(function=_validate_file_upload,
                          file_bucket=self._get_file_bucketname(),
                          file_key=file_key,
                          objsize=fileinfo.filesize(),
                          checksum=fileinfo.checksum())

            ospar = _ObjectStore.create_par(bucket=file_bucket,
                                            encrypt_key=encrypt_key,
                                            key=file_key,
                                            readable=False,
                                            writeable=True,
                                            cleanup_function=f)
        else:
            ospar = None

        # now save the fileinfo to the object store
        fileinfo.save()
        filemeta = fileinfo.get_filemeta()

        # return the PAR if we need to have a second-stage of upload
        return (filemeta, ospar)

    def download(self, filename, authorisation,
                 version=None, encrypt_key=None,
                 force_par=False, must_chunk=False,
                 par=None, identifiers=None):
        """Download the file called filename. This will return a
           FileHandle that describes the file. If the file is
           sufficiently small, then the filedata will be embedded
           into this handle. Otherwise a PAR will be generated and
           also returned to allow the file to be downloaded
           separately. The PAR will be encrypted with 'encrypt_key'.
           Remember to close the PAR once you have finished
           downloading the file...
        """
        from Acquire.Storage import FileHandle as _FileHandle
        from Acquire.Storage import FileInfo as _FileInfo
        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        if not isinstance(encrypt_key, _PublicKey):
            raise TypeError("The encryption key must be of type PublicKey")

        (drive_acl, identifiers) = self._resolve_acl(
                    authorisation=authorisation,
                    resource="download %s %s" % (self._drive_uid, filename),
                    par=par, identifiers=identifiers)

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

        file_key = fileinfo.version()._file_key()
        filedata = None
        downloader = None
        ospar = None

        if fileinfo.version().is_chunked():
            # this is a chunked file. We need to return a
            # ChunkDownloader to download the file
            from Acquire.Client import ChunkDownloader as _ChunkDownloader
            downloader = _ChunkDownloader(drive_uid=self._drive_uid,
                                          file_uid=fileinfo.version().uid())

            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket

            bucket = _get_service_account_bucket()
            key = "%s/%s/%s/%s" % (_downloader_root, self._drive_uid,
                                   filemeta.uid(), downloader.uid())

            data = {"filename": filename,
                    "version": filemeta.uid(),
                    "filekey": fileinfo.version()._file_key(),
                    "secret": downloader.secret()}

            _ObjectStore.set_object_from_json(bucket, key, data)

        elif must_chunk:
            raise PermissionError(
                "Cannot download this file in a chunked manner!")

        elif force_par or fileinfo.filesize() > 1048576:
            # the file is too large to include in the download so
            # we need to use a OSPar to download
            ospar = _ObjectStore.create_par(bucket=file_bucket,
                                            encrypt_key=encrypt_key,
                                            key=file_key,
                                            readable=True,
                                            writeable=False)
        else:
            # one-trip download of files that are less than 1 MB
            filedata = _ObjectStore.get_object(file_bucket, file_key)

        # return the filemeta, and either the filedata, ospar or downloader
        return (filemeta, filedata, ospar, downloader)

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

        drive_acl = self.aclrules().resolve(identifiers=identifiers,
                                            upstream=upstream)

        return drive_acl.is_owner()

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
        self.load(autocreate=False)

        if not self.is_opened_by_owner():
            raise PermissionError(
                "You cannot change user permissions as you are either "
                "not the owner of this drive or you failed to provide "
                "authorisation when you opened the drive")

        # this will append the new rules, ensuring that the change
        # does not leave the drive ownerless
        self._aclrules.append(aclrules, ensure_owner=True)

        self.save()
        self.load(autocreate=False)

    def list_files(self, authorisation=None, par=None,
                   identifiers=None, include_metadata=False,
                   dir=None, filename=None):
        """Return the list of FileMeta data for the files contained
           in this Drive. The passed authorisation is needed in case
           the list contents of this drive is not public.

           If 'dir' is specified, then only search for files in 'dir'.
           If 'filename' is specified, then only search for the
           file called 'filename'
        """
        (drive_acl, identifiers) = self._resolve_acl(
                                        authorisation=authorisation,
                                        resource="list_files",
                                        par=par, identifiers=identifiers)

        if par is not None:
            if par.location().is_file():
                dir = None
                filename = par.location().filename()
            elif not par.location().is_drive():
                raise PermissionError(
                    "You do not have permission to read the Drive")

        if not drive_acl.is_readable():
            raise PermissionError(
                "You don't have permission to read this Drive")

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import encoded_to_string as _encoded_to_string
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from Acquire.Storage import FileMeta as _FileMeta

        metadata_bucket = self._get_metadata_bucket()

        if filename is not None:
            key = "%s/%s/%s" % (_fileinfo_root, self._drive_uid,
                                _string_to_encoded(filename))

            names = [key]
        else:
            key = "%s/%s" % (_fileinfo_root, self._drive_uid)

            if dir is not None:
                key = "%s/%s" % (key, _string_to_encoded(dir))

            names = _ObjectStore.get_all_object_names(metadata_bucket, key)

        files = []

        if include_metadata:
            # we need to load all of the metadata info for this file to
            # return to the user
            from Acquire.Storage import FileInfo as _FileInfo

            for name in names:
                try:
                    data = _ObjectStore.get_object_from_json(metadata_bucket,
                                                             name)
                    fileinfo = _FileInfo.from_data(data,
                                                   identifiers=identifiers,
                                                   upstream=drive_acl)
                    filemeta = fileinfo.get_filemeta()
                    file_acl = filemeta.acl()

                    if file_acl.is_readable() or file_acl.is_writeable():
                        files.append(filemeta)
                except:
                    pass
        else:
            for name in names:
                filename = _encoded_to_string(name.split("/")[-1])
                files.append(_FileMeta(filename=filename))

        return files

    def list_versions(self, filename, authorisation=None,
                      include_metadata=False, par=None,
                      identifiers=None):
        """Return the list of versions of the file with specified
           filename. If 'include_metadata' is true then this will
           load full metadata for each version. This will return
           a sorted list of FileMeta objects. The passed authorisation
           is needed in case the version info is not public
        """
        (drive_acl, identifiers) = self._resolve_acl(
                                    authorisation=authorisation,
                                    resource="list_versions %s" % filename,
                                    par=par, identifiers=identifiers)

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

    def load(self, autocreate=False):
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
            if not autocreate:
                raise PermissionError(
                    "There is no drive available with UID '%s'"
                    % self._drive_uid)

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
