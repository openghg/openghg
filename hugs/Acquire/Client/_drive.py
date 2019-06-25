
__all__ = ["Drive"]


def _create_drive(metadata, creds):
    """Internal function used to create a Drive"""
    drive = Drive()
    drive._creds = creds

    from copy import copy as _copy
    metadata = _copy(metadata)
    metadata._set_credentials(creds)
    drive._metadata = metadata

    return drive


def _get_drive(creds, name=None, drive_uid=None, autocreate=True):
    """Return the drive called 'name' using the passed credentials. The name
       will default to 'main' if it is not set, and the drive will
       be created automatically is 'autocreate' is True and the
       drive does not exist
    """
    storage_service = creds.storage_service()

    if drive_uid is None:
        if name is None:
            name = "main"
        else:
            name = str(name)

        if autocreate:
            autocreate = True
        else:
            autocreate = False
    else:
        name = None
        autocreate = False
        drive_uid = str(drive_uid)

    args = {"name": name, "autocreate": autocreate,
            "drive_uid": drive_uid}

    if creds.is_user():
        from Acquire.Client import Authorisation as _Authorisation
        authorisation = _Authorisation(resource="UserDrives",
                                       user=creds.user())
        args["authorisation"] = authorisation.to_data()
    elif creds.is_par():
        par = creds.par()
        par.assert_valid()
        args["par_uid"] = par.uid()
        args["secret"] = creds.secret()

    response = storage_service.call_function(function="open_drive", args=args)

    from Acquire.Client import DriveMeta as _DriveMeta

    return _create_drive(creds=creds,
                         metadata=_DriveMeta.from_data(response["drive"]))


class Drive:
    """This class provides a handle to a user's drive (space
       to hold files and folders). A drive is associated with
       a single storage service and can be shared amongst several
       users. Each drive has a unique UID, with users assiging
       their own shorthand names.

    """
    def __init__(self, name=None, drive_uid=None, creds=None, autocreate=True):
        """Construct a handle to the drive that the passed user
           calls 'name' on the passed storage service. If
           'autocreate' is True and the user is logged in then
           this will automatically create the drive if
           it doesn't exist already
        """
        self._metadata = None
        self._creds = None

        if creds is not None:
            from Acquire.Client import StorageCreds as _StorageCreds
            if not isinstance(creds, _StorageCreds):
                raise TypeError("creds must be type StorageCreds")

            drive = _get_drive(creds=creds, name=name, drive_uid=drive_uid,
                               autocreate=autocreate)

            from copy import copy as _copy
            self.__dict__ = _copy(drive.__dict__)

    @staticmethod
    def open(metadata=None, drive_uid=None, creds=None):
        """Open and return the drive from the passed DriveMeta. The
           drive either needs to be opened via the User with
           storage service, or by passing in a valid PAR and secret
        """
        from Acquire.Client import DriveMeta as _DriveMeta
        from Acquire.Client import StorageCreds as _StorageCreds

        if not isinstance(metadata, _DriveMeta):
            raise TypeError("The metadata must be type DriveMeta")

        if metadata is None:
            return Drive(drive_uid=drive_uid, creds=creds)

        if creds is None:
            creds = metadata.credentials()

        if not isinstance(creds, _StorageCreds):
            raise TypeError("The creds must be type StorageCreds")

        return _create_drive(creds=creds, metadata=metadata)

    def __str__(self):
        if self.is_null():
            return "Drive::null"
        else:
            return "Drive(name='%s')" % self._metadata.name()

    def is_null(self):
        """Return whether or not this drive is null

           Returns:
                bool: True if null, else False
        """
        return self._metadata is None

    def metadata(self):
        """Return the metadata about this drive"""
        if self.is_null():
            return None
        else:
            from copy import copy as _copy
            return _copy(self._metadata)

    def credentials(self):
        """Return the credentials used to open this drive"""
        if self.is_null():
            return None
        else:
            return self._creds

    def storage_service(self):
        """Return the storage service for this drive
        Returns:
            Service: Storage service for drive
        """
        if self.is_null():
            return None
        else:
            return self._creds.storage_service()

    def chunk_upload(self, filename, dir=None, aclrules=None):
        """Start a chunked upload of a file called 'filename' (just the
           filename - not the full path - if you want to specify a certain
           directory in the Drive then specify that in 'dir').
           The file will be uploaded to the Drive at
           'dir/filename'. If a file with this name exists,
           then this will upload a new version (assuming you have permission).
           Otherwise this will create a new file. You can set the
           ACL rules used to grant access to this file via 'aclrules'.
           If this is not set, then the rules will be derived from either
           the last version of the file, or inherited from the drive.

           This will return a ChunkUploader which can be used to actually
           upload the file
        """
        if self.is_null():
            raise PermissionError("Cannot upload a file to a null drive!")

        if dir is not None:
            filename = "%s/%s" % (dir, filename)

        from Acquire.Client import FileMeta as _FileMeta
        filemeta = _FileMeta(filename=filename)
        filemeta._set_drive_metadata(self._metadata, self._creds)

        return filemeta.open().chunk_upload(aclrules=aclrules)

    def upload(self, filename, dir=None, uploaded_name=None, aclrules=None,
               force_par=False):
        """Upload the file at 'filename' to this drive, assuming we have
           write access to this drive. The local file 'filename' will be
           uploaded to the drive as the file called 'filename' (just the
           filename - not the full path - if you want to specify a certain
           directory in the Drive then specify that in 'dir').
           If you want to specify the uploaded name then set this as
           "uploaded_name". The file will be uploaded to the Drive at
           'dir/uploaded_name'. If a file with this name exists,
           then this will upload a new version (assuming you have permission).
           Otherwise this will create a new file. You can set the
           ACL rules used to grant access to this file via 'aclrules'.
           If this is not set, then the rules will be derived from either
           the last version of the file, or inherited from the drive.
        """
        if self.is_null():
            raise PermissionError("Cannot upload a file to a null drive!")

        if uploaded_name is None:
            import os as _os
            uploaded_name = _os.path.split(filename)[1]

        if dir is not None:
            uploaded_name = "%s/%s" % (dir, uploaded_name)

        from Acquire.Client import FileMeta as _FileMeta
        filemeta = _FileMeta(filename=uploaded_name)
        filemeta._set_drive_metadata(self._metadata, self._creds)

        return filemeta.open().upload(filename=filename, force_par=force_par,
                                      aclrules=aclrules)

    def chunk_download(self, filename, dir=None, download_name=None,
                       version=None):
        """Download the file 'filename' from the Drive to directory 'dir' on
           this computer (or current directory if not specified), calling
           the downloaded file 'download_filename' (or 'filename' if not
           specified). Force transfer using an OSPar is force_par is True
        """
        if self.is_null():
            raise PermissionError("Cannot upload a file to a null drive!")

        from Acquire.Client import FileMeta as _FileMeta
        filemeta = _FileMeta(filename=filename)
        filemeta._set_drive_metadata(self._metadata, self._creds)

        return filemeta.open().chunk_download(filename=download_name,
                                              version=version, dir=dir)

    def download(self, filename, dir=None, download_name=None,
                 version=None, force_par=False):
        """Download the file 'filename' from the Drive to directory 'dir' on
           this computer (or current directory if not specified), calling
           the downloaded file 'download_filename' (or 'filename' if not
           specified). Force transfer using an OSPar is force_par is True
        """
        if self.is_null():
            raise PermissionError("Cannot upload a file to a null drive!")

        from Acquire.Client import FileMeta as _FileMeta
        filemeta = _FileMeta(filename=filename)
        filemeta._set_drive_metadata(self._metadata, self._creds)

        return filemeta.open().download(filename=download_name,
                                        version=version, dir=dir,
                                        force_par=force_par)

    @staticmethod
    def _list_drives(creds, drive_uid=None):
        """Return a list of all of the DriveMetas of the drives accessible
           at the top-level using the passed credentials, or that are
           sub-drives of the drive with UID 'drive_uid'
        """
        from Acquire.Client import StorageCreds as _StorageCreds
        if not isinstance(creds, _StorageCreds):
            raise TypeError("The passed creds must be type StorageCreds")

        args = {}

        if creds.is_user():
            if drive_uid is not None:
                drive_uid = str(drive_uid)

            from Acquire.Client import Authorisation as _Authorisation
            authorisation = _Authorisation(resource="UserDrives",
                                           user=creds.user())

            args = {"authorisation": authorisation.to_data()}
        elif creds.is_par():
            par = creds.par()
            par.assert_valid()
            args["par"] = par.to_data()
            args["secret"] = creds.secret()

        if drive_uid is not None:
            args["drive_uid"] = str(drive_uid)

        storage_service = creds.storage_service()

        response = storage_service.call_function(
                                    function="list_drives", args=args)

        from Acquire.ObjectStore import string_to_list as _string_to_list
        from Acquire.Client import DriveMeta as _DriveMeta

        drives = _string_to_list(response["drives"], _DriveMeta)

        for drive in drives:
            drive._creds = creds

        return drives

    @staticmethod
    def list_toplevel_drives(creds):
        """Return a list of all of the DriveMetas of the drives accessible
           at the top-level using the passed credentils
        """
        return Drive._list_drives(creds=creds)

    def list_drives(self):
        """Return a list of the DriveMetas of all of the drives contained
           in this drive that are accessible to the user

           Returns:
                list: List of DriveMetas for the drives
        """
        if self.is_null():
            return []
        else:
            return Drive._list_drives(drive_uid=self._metadata.uid(),
                                      creds=self._creds)

    def list_files(self, dir=None, filename=None, include_metadata=False):
        """Return a list of the FileMetas of all of the files contained
           in this drive. If 'dir' is specified then list only the
           files that are contained in 'dir'. If 'filename' is specified
           then return only the files that match the passed filename
        """
        if self.is_null():
            return []

        from Acquire.ObjectStore import string_to_list as _string_to_list
        from Acquire.Storage import FileMeta as _FileMeta

        if include_metadata:
            include_metadata = True
        else:
            include_metadata = False

        args = {"drive_uid": self._metadata.uid(),
                "include_metadata": include_metadata}

        if dir is not None:
            args["dir"] = str(dir)

        if filename is not None:
            args["filename"] = str(filename)

        if self._creds.is_user():
            from Acquire.Client import Authorisation as _Authorisation
            authorisation = _Authorisation(resource="list_files",
                                           user=self._creds.user())
            args["authorisation"] = authorisation.to_data()
        elif self._creds.is_par():
            par = self._creds.par()
            par.assert_valid()
            args["par_uid"] = par.uid()
            args["secret"] = self._creds.secret()

        response = self.storage_service().call_function(function="list_files",
                                                        args=args)

        files = _string_to_list(response["files"], _FileMeta)

        for f in files:
            f._set_drive_metadata(self._metadata, self._creds)

        return files
