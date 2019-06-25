
__all__ = ["File"]


class File:
    """This class provides a handle to a user's file on a Drive.
       Files exist on Drives, and are read-only on the Drive.
       You update a File by uploading a new version
    """
    def __init__(self):
        self._metadata = None

    def is_null(self):
        """Return whether or not this File is null"""
        return self._metadata is None

    @staticmethod
    def open(metadata, creds=None):
        """Open and return the File from the passed FileMeta. The
           file either needs to be opened by the User with
           specified storage service, or by passing in a
           valid PAR and secret
        """
        from Acquire.Client import FileMeta as _FileMeta

        if not isinstance(metadata, _FileMeta):
            raise TypeError("The metadata must be type FileMeta")

        if creds is not None:
            from Acquire.Client import StorageCreds as _StorageCreds
            if not isinstance(creds, _StorageCreds):
                raise TypeError("The creds must be type StorageCreds")

        from copy import copy as _copy
        metadata = _copy(metadata)

        if creds is None:
            creds = metadata._creds
        else:
            metadata._creds = creds

        if creds.is_par():
            par = creds.par()
            loc = par.location()
            if not loc.is_drive():
                # we need to create a dummy Drive metadata for this file
                from Acquire.Client import DriveMeta as _DriveMeta
                d = _DriveMeta(name="par:%s" % par.uid(),
                               uid=loc.drive_uid())
                metadata._set_drive_metadata(d, creds)

        f = File()
        f._metadata = metadata
        f._creds = creds

        return f

    def metadata(self):
        """Return the metadata for this file"""
        if self.is_null():
            return None
        else:
            from copy import copy as _copy
            return _copy(self._metadata)

    def credentials(self):
        """Return the credentials used to open this file"""
        if self.is_null():
            return None
        else:
            return self._creds

    def __str__(self):
        if self.is_null():
            return "File::null"
        else:
            return "File(name='%s')" % self._metadata.name()

    def chunk_upload(self, aclrules=None):
        """Start a chunk-upload of a new version of this file. This
           will return a chunk-uploader that can be used to upload
           a file chunk-by-chunk
        """
        if self.is_null():
            raise PermissionError("Cannot download a null File!")

        if self._creds is None:
            raise PermissionError("We have not properly opened the file!")

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.ObjectStore import OSPar as _OSPar
        from Acquire.Client import FileMeta as _FileMeta

        uploaded_name = self._metadata.filename()
        drive_uid = self._metadata.drive().uid()

        args = {"filename": uploaded_name,
                "drive_uid": drive_uid}

        if aclrules is not None:
            args["aclrules"] = aclrules.to_data()

        if self._creds.is_user():
            authorisation = _Authorisation(
                        resource="chunk_upload %s" % uploaded_name,
                        user=self._creds.user())

            args["authorisation"] = authorisation.to_data()
        elif self._creds.is_par():
            par = self._creds.par()
            par.assert_valid()
            args["par_uid"] = par.uid()
            args["secret"] = self._creds.secret()
        else:
            raise PermissionError(
                "Either a logged-in user or valid PAR must be provided!")

        if self._creds.is_user():
            privkey = self._creds.user().session_key()
        else:
            from Acquire.Crypto import get_private_key \
                as _get_private_key
            privkey = _get_private_key("parkey")

        args["encryption_key"] = privkey.public_key().to_data()

        # will eventually need to authorise payment...
        storage_service = self._creds.storage_service()

        response = storage_service.call_function(
                                function="open_uploader", args=args)

        filemeta = _FileMeta.from_data(response["filemeta"])

        filemeta._set_drive_metadata(self._metadata._drive_metadata,
                                     self._creds)

        self._metadata = filemeta

        from Acquire.Client import ChunkUploader as _ChunkUploader
        return _ChunkUploader.from_data(response["uploader"],
                                        privkey=privkey,
                                        service=storage_service)

    def upload(self, filename, force_par=False, aclrules=None):
        """Upload 'filename' as the new version of this file"""
        if self.is_null():
            raise PermissionError("Cannot download a null File!")

        if self._creds is None:
            raise PermissionError("We have not properly opened the file!")

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.ObjectStore import OSPar as _OSPar
        from Acquire.Client import FileMeta as _FileMeta
        from Acquire.Storage import FileHandle as _FileHandle

        local_cutoff = None

        if force_par:
            # only upload using a OSPar
            local_cutoff = 0

        uploaded_name = self._metadata.filename()
        drive_uid = self._metadata.drive().uid()

        filehandle = _FileHandle(filename=filename,
                                 remote_filename=uploaded_name,
                                 drive_uid=drive_uid,
                                 aclrules=aclrules,
                                 local_cutoff=local_cutoff)

        try:
            args = {"filehandle": filehandle.to_data()}

            if self._creds.is_user():
                authorisation = _Authorisation(
                            resource="upload %s" % filehandle.fingerprint(),
                            user=self._creds.user())

                args["authorisation"] = authorisation.to_data()
            elif self._creds.is_par():
                par = self._creds.par()
                par.assert_valid()
                args["par_uid"] = par.uid()
                args["secret"] = self._creds.secret()
            else:
                raise PermissionError(
                    "Either a logged-in user or valid PAR must be provided!")

            if not filehandle.is_localdata():
                # we will need to upload against a OSPar, so need to tell
                # the service how to encrypt the OSPar...
                if self._creds.is_user():
                    privkey = self._creds.user().session_key()
                else:
                    from Acquire.Crypto import get_private_key \
                        as _get_private_key
                    privkey = _get_private_key("parkey")

                args["encryption_key"] = privkey.public_key().to_data()

            # will eventually need to authorise payment...
            storage_service = self._creds.storage_service()

            response = storage_service.call_function(
                                    function="upload", args=args)

            filemeta = _FileMeta.from_data(response["filemeta"])

            # if this was a large file, then we will receive a OSPar back
            # which must be used to upload the file
            if not filehandle.is_localdata():
                par = _OSPar.from_data(response["upload_par"])
                par.write(privkey).set_object_from_file(
                                        filehandle.local_filename())
                par.close(privkey)

            filemeta._set_drive_metadata(self._metadata._drive_metadata,
                                         self._creds)

            return filemeta
        except:
            # ensure that any temporary files are removed
            filehandle.__del__()
            raise

    def chunk_download(self, filename=None, version=None,
                       dir=None):
        """Return a ChunkDownloader to download this file
           chunk-by-chunk
        """
        if self.is_null():
            raise PermissionError("Cannot download a null File!")

        if self._creds is None:
            raise PermissionError("We have not properly opened the file!")

        if filename is None:
            filename = self._metadata.name()

        drive_uid = self._metadata.drive().uid()

        if self._creds.is_user():
            privkey = self._creds.user().session_key()
        else:
            from Acquire.Crypto import get_private_key as _get_private_key
            privkey = _get_private_key("parkey")

        args = {"drive_uid": drive_uid,
                "filename": self._metadata.name(),
                "encryption_key": privkey.public_key().to_data(),
                "must_chunk": True}

        if self._creds.is_user():
            from Acquire.Client import Authorisation as _Authorisation
            authorisation = _Authorisation(
                        resource="download %s %s" % (drive_uid,
                                                     self._metadata.name()),
                        user=self._creds.user())
            args["authorisation"] = authorisation.to_data()
        elif self._creds.is_par():
            par = self._creds.par()
            par.assert_valid()
            args["par_uid"] = par.uid()
            args["secret"] = self._creds.secret()

        storage_service = self._creds.storage_service()

        response = storage_service.call_function(
                                function="download", args=args)

        from Acquire.Client import ChunkDownloader as _ChunkDownloader
        downloader = _ChunkDownloader.from_data(response["downloader"],
                                                privkey=privkey,
                                                service=storage_service)

        downloader._start_download(filename=filename, dir=dir)

        return downloader

    def download(self, filename=None, version=None,
                 dir=None, force_par=False):
        """Download this file into the local directory
           the local directory, or 'dir' if specified,
           calling the file 'filename' (or whatever it is called
           on the Drive if not specified). If a local
           file exists with this name, then a new, unique filename
           will be used. This returns the local filename of the
           downloaded file (with full absolute path)

           Note that this only downloads files for which you
           have read-access. If the file is not readable then
           an exception is raised and nothing is returned

           If 'version' is specified then download a specific version
           of the file. Otherwise download the version associated
           with this file object
        """
        if self.is_null():
            raise PermissionError("Cannot download a null File!")

        if self._creds is None:
            raise PermissionError("We have not properly opened the file!")

        if filename is None:
            filename = self._metadata.name()

        drive_uid = self._metadata.drive().uid()

        from Acquire.Client import create_new_file as \
            _create_new_file

        if self._creds.is_user():
            privkey = self._creds.user().session_key()
        else:
            from Acquire.Crypto import get_private_key as _get_private_key
            privkey = _get_private_key("parkey")

        args = {"drive_uid": drive_uid,
                "filename": self._metadata.name(),
                "encryption_key": privkey.public_key().to_data()}

        if self._creds.is_user():
            from Acquire.Client import Authorisation as _Authorisation
            authorisation = _Authorisation(
                        resource="download %s %s" % (drive_uid,
                                                     self._metadata.name()),
                        user=self._creds.user())
            args["authorisation"] = authorisation.to_data()
        elif self._creds.is_par():
            par = self._creds.par()
            par.assert_valid()
            args["par_uid"] = par.uid()
            args["secret"] = self._creds.secret()

        if force_par:
            args["force_par"] = True

        if version is not None:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            args["version"] = _datetime_to_string(version)
        elif self._metadata.version() is not None:
            args["version"] = self._metadata.version()

        storage_service = self._creds.storage_service()

        response = storage_service.call_function(
                                function="download", args=args)

        from Acquire.Client import FileMeta as _FileMeta
        filemeta = _FileMeta.from_data(response["filemeta"])

        if "filedata" in response:
            # we have already downloaded the file to 'filedata'
            filedata = response["filedata"]

            from Acquire.ObjectStore import string_to_bytes \
                as _string_to_bytes
            filedata = _string_to_bytes(response["filedata"])
            del response["filedata"]

            # validate that the size and checksum are correct
            filemeta.assert_correct_data(filedata)

            if filemeta.is_compressed():
                # uncompress the data
                from Acquire.Client import uncompress as _uncompress
                filedata = _uncompress(
                                inputdata=filedata,
                                compression_type=filemeta.compression_type())

            # write the data to the specified local file...
            filename = _create_new_file(filename=filename, dir=dir)
            with open(filename, "wb") as FILE:
                FILE.write(filedata)
                FILE.flush()
        elif "download_par" in response:
            from Acquire.ObjectStore import OSPar as _OSPar
            filename = _create_new_file(filename=filename, dir=dir)
            par = _OSPar.from_data(response["download_par"])
            par.read(privkey).get_object_as_file(filename)
            par.close(privkey)

            # validate that the size and checksum are correct
            filemeta.assert_correct_data(filename=filename)

            # uncompress the file if desired
            if filemeta.is_compressed():
                from Acquire.Client import uncompress as _uncompress
                _uncompress(inputfile=filename,
                            outputfile=filename,
                            compression_type=filemeta.compression_type())

        elif "downloader" in response:
            from Acquire.Client import ChunkDownloader as _ChunkDownloader
            downloader = _ChunkDownloader.from_data(response["downloader"],
                                                    privkey=privkey,
                                                    service=storage_service)

            filename = downloader.download(filename=filename, dir=dir)

        filemeta._copy_credentials(self._metadata)
        self._metadata = filemeta

        return filename

    def list_versions(self, include_metadata=False):
        """Return a list of all of the versions of this file.
           If 'include_metadata' is True then this will include
           the full metadata of every version
        """
        if self.is_null():
            return []

        if self._creds is None:
            raise PermissionError(
                "Cannot list versions of this file as it has not "
                "been properly opened")

        drive_uid = self._metadata.drive().uid()

        if include_metadata:
            include_metadata = True
        else:
            include_metadata = False

        filename = self._metadata.name()

        args = {"drive_uid": drive_uid,
                "include_metadata": include_metadata,
                "filename": filename}

        if self._creds.is_user():
            from Acquire.Client import Authorisation as _Authorisation
            authorisation = _Authorisation(
                                    resource="list_versions %s" % filename,
                                    user=self._creds.user())
            args["authorisation"] = authorisation.to_data()
        elif self._creds.is_par():
            par = self._creds.par()
            par.assert_valid()
            args["par_uid"] = par.uid()
            args["secret"] = self._creds.secret()

        storage_service = self._creds.storage_service()

        response = storage_service.call_function(function="list_versions",
                                                 args=args)

        from Acquire.ObjectStore import string_to_list \
            as _string_to_list
        from Acquire.Storage import FileMeta as _FileMeta

        versions = _string_to_list(response["versions"], _FileMeta)

        for version in versions:
            version._copy_credentials(self._metadata)

        return versions
