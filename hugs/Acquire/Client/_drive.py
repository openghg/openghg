
__all__ = ["Drive"]


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

    return service


def _create_drive(user, name, drivemeta, storage_service):
    """Internal function used to create a Drive
    
       Args:
            user (User): User for drive
            name (str): Name for drive
            drivemeta (DriveMeta): Object containing
            metadata for drive
            storage_service (Service): Service for drive
       Returns:
            Drive: Drive object
    """
    drive = Drive()
    drive._name = drivemeta.name()
    drive._drive_uid = drivemeta.uid()
    drive._container = drivemeta.container_uids()
    drive._acl = drivemeta.acl()
    drive._aclrules = drivemeta.aclrules()
    drive._user = user
    drive._storage_service = storage_service
    return drive


def _get_drive(user, name=None, storage_service=None, storage_url=None,
               autocreate=True):
    """Return the drive called 'name' of the passed user. Note that the
       user must be authenticated to call this function. The name
       will default to 'main' if it is not set, and the drive will
       be created automatically is 'autocreate' is True and the
       drive does not exist

       Args:
            user (User): User to use drive
            name (str, default=None): Name for drive
            storage_service (Service, default=None): Service object to use
            storage_url (str, default=None): URL for storage
            autocreate (bool): If True create drive automatically, 
            if False do not
       Returns:
            Drive: Drive object

    """
    if storage_service is None:
        storage_service = _get_storage_service(storage_url)
    else:
        if not storage_service.is_storage_service():
            raise TypeError("You can only query drives using "
                            "a valid storage service")

    if name is None:
        name = "main"
    else:
        name = str(name)

    if autocreate:
        autocreate = True
    else:
        autocreate = False

    from Acquire.Client import Authorisation as _Authorisation
    authorisation = _Authorisation(resource="UserDrives", user=user)

    args = {"authorisation": authorisation.to_data(),
            "name": name, "autocreate": autocreate}

    response = storage_service.call_function(function="open_drive", args=args)

    from Acquire.Client import DriveMeta as _DriveMeta

    return _create_drive(user=user, name=name, storage_service=storage_service,
                         drivemeta=_DriveMeta.from_data(response["drive"]))


class Drive:
    """This class provides a handle to a user's drive (space
       to hold files and folders). A drive is associated with
       a single storage service and can be shared amongst several
       users. Each drive has a unique UID, with users assiging
       their own shorthand names.
       
    """
    def __init__(self, user=None, name=None, storage_service=None,
                 storage_url=None, autocreate=True):
        """Construct a handle to the drive that the passed user
           calls 'name' on the passed storage service. If
           'autocreate' is True and the user is logged in then
           this will automatically create the drive if
           it doesn't exist already
        """
        if user is not None:
            drive = _get_drive(user=user, name=name,
                               storage_service=storage_service,
                               storage_url=storage_url, autocreate=autocreate)

            from copy import copy as _copy
            self.__dict__ = _copy(drive.__dict__)
        else:
            self._drive_uid = None

    def __str__(self):
        if self.is_null():
            return "Drive::null"
        else:
            return "Drive(user='%s', name='%s')" % \
                    (self._user.username(), self.name())

    def is_null(self):
        """Return whether or not this drive is null
        
           Returns:
                bool: True if null, else False
        """
        return self._drive_uid is None

    def acl(self):
        """Return the access control list for the user on this drive.
           If the ACL is not known, then None is returned

           Returns:
                str: Access Control List for the user

        """
        try:
            return self._acl
        except:
            return None

    def aclrules(self):
        """Return the ACL rules used to grant access to this drive. This
           is only visible to owners of this drive. If it is not visible,
           then None is returned

           Returns:
                str: ACL rules for the drive
        """
        try:
            return self._aclrules
        except:
            return None

    def name(self):
        """Return the name given to this drive by the user
        
           Returns:
                str: Name of drive
        """
        return self._name

    def uid(self):
        """Return the UID of this drive
           
           Returns:
                str: UID of drive
                
        """
        return self._drive_uid

    def guid(self):
        """Return the global UID of this drive (combination of the
           UID of the storage service and UID of the drive)

           Returns:
                str: Global UID for drive

        """
        if self.is_null():
            return None
        else:
            return "%s@%s" % (self.storage_service().uid(), self.uid())

    def storage_service(self):
        """Return the storage service for this drive
        
        Returns:
            Service: Storage service for drive
        
        
        """
        if self.is_null():
            return None
        else:
            return self._storage_service

    def bulk_upload(self, max_size=None, aclrules=None):
        """Start the bulk upload of a large number of files to this
           drive, assuming we have write access to this drive. This
           will return a bulk upload PAR that can be used to write to a bucket.
           All of the files written to this bucket will be copied into
           this drive using the (optionally) supplied aclrules
           to control access (or "inherit" if no rules are supplied).

            Once you have finished uploading, you must call the
            "close" function on the BulkUploadPAR so that the files
            are correctly copied. The filenames as they are written
            to the PAR will be used, creating new files (and subdrives)
            as needed

            Args:
                max_size (int, default=None): Max size for upload
                aclrules (str, default=None): ACL rules for upload
            Returns:
                str: Name of drive
        """
        if self.is_null():
            raise PermissionError(
                "Cannot perform a bulk upload of files to a null drive")

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.Client import PAR as _PAR
        from Acquire.Crypto import PrivateKey as _PrivateKey

        authorisation = _Authorisation(
                            resource="bulk_upload %s %s" %
                            (self._drive_uid, max_size), user=self._user)

        # we need a new private key to secure access to this PAR
        privkey = _PrivateKey()

        args = {"drive_uid": self._drive_uid,
                "authorisation": authorisation.to_data(),
                "encrypt_key": privkey.public_key().to_data()}

        if aclrules is not None:
            args["aclrules"] = aclrules.to_data()

        if max_size is not None:
            args["max_size"] = max_size

        # will eventually need to authorise payment...

        response = self.storage_service().call_function(
                                function="bulk_upload", args=args)

        par = _PAR.from_data(response["bulk_upload_par"])

        par._set_private_key(privkey)

        return par

    def upload(self, filename, uploaded_name=None, aclrules=None,
               force_par=False):
        """Upload the file at 'filename' to this drive, assuming we have
           write access to this drive. The local file 'filename' will be
           uploaded to the drive as the file called 'filename' (just the
           filename - not the full path). If you want to specify the
           uploaded name then set this as "uploaded_name" (which again will
           just be a filename - no paths). If a file with this name exists,
           then this will upload a new version (assuming you have permission).
           Otherwise this will create a new file. You can set the
           ACL rules used to grant access to this file via 'aclrule'.
           If this is not set, then the rules will be derived from either
           the last version of the file, or inherited from the drive.

           Args:
                filename (str): Name of file to upload
                uploaded_name (str, default=None): Name of file once uploaded
                aclrules (str, default=None): ACL rules for file
                force_par (bool, default=False): If True force a pre-authenticated
                request be created for the upload
            Returns:
                FileMeta: Object containing metadata on the uploaded file
        """
        if self.is_null():
            raise PermissionError("Cannot upload a file to a null drive!")

        if uploaded_name is None:
            uploaded_name = filename

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.Client import FileHandle as _FileHandle
        from Acquire.Client import PAR as _PAR
        from Acquire.Client import FileMeta as _FileMeta

        local_cutoff = None

        if force_par:
            # only upload using a PAR
            local_cutoff = 0

        filehandle = _FileHandle(filename=filename,
                                 remote_filename=uploaded_name,
                                 drive_uid=self.uid(),
                                 aclrules=aclrules,
                                 local_cutoff=local_cutoff)

        try:
            authorisation = _Authorisation(
                            resource="upload %s" % filehandle.fingerprint(),
                            user=self._user)

            args = {"filehandle": filehandle.to_data(),
                    "authorisation": authorisation.to_data()}

            if not filehandle.is_localdata():
                # we will need to upload against a PAR, so need to tell
                # the service how to encrypt the PAR...
                privkey = self._user.session_key()
                args["encryption_key"] = privkey.public_key().to_data()

            # will eventually need to authorise payment...

            response = self.storage_service().call_function(
                                    function="upload", args=args)

            filemeta = _FileMeta.from_data(response["filemeta"])

            # if this was a large file, then we will receive a PAR back
            # which must be used to upload the file
            if not filehandle.is_localdata():
                par = _PAR.from_data(response["upload_par"])
                par.write(privkey).set_object_from_file(
                                        filehandle.local_filename())
                par.close(privkey)

            return filemeta
        except:
            # ensure that any temporary files are removed
            filehandle.__del__()
            raise

    def download(self, filename, downloaded_name=None, version=None,
                 dir=None, force_par=False):
        """Download the file called 'filename' from this drive into
           the local directory, or 'dir' if specified,
           ideally called 'filename'
           (or 'downloaded_name' if that is specified). If a local
           file exists with this name, then a new, unique filename
           will be used. This returns a dictionary mapping the
           downloaded filename to the FileMeta of the file

           Note that this only downloads files for which you
           have read-access. If the file is not readable then
           an exception is raised and nothing is returned

           If 'version' is specified then download a specific version
           of the file. Otherwise download the latest version

           Args:
                filename (str): Name of file to download
                downloaded_name (str, default=None): Name of file once downloaded
                version (datetime, default=None): Datetime denoting version 
                of file to use
                dir (str, default=None): Directory for file
                force_par (bool, default=False): If True force a pre-authenticated
                request be created for the download
           Returns:
                FileMeta: Object containing metadata on the uploaded file

        """
        if self.is_null():
            raise PermissionError("Cannot download from a null drive!")

        if downloaded_name is None:
            downloaded_name = filename

        from Acquire.Client import create_new_file as \
            _create_new_file

        downloaded_name = _create_new_file(filename=downloaded_name,
                                           dir=dir)

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.Client import FileMeta as _FileMeta

        authorisation = _Authorisation(
                            resource="download %s %s" % (self.uid(),
                                                         filename),
                            user=self._user)

        privkey = self._user.session_key()

        args = {"drive_uid": self.uid(),
                "filename": filename,
                "authorisation": authorisation.to_data(),
                "encryption_key": privkey.public_key().to_data()}

        if force_par:
            args["force_par"] = True

        if version is not None:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            args["version"] = _datetime_to_string(version)

        response = self.storage_service().call_function(
                                function="download", args=args)

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
            with open(downloaded_name, "wb") as FILE:
                FILE.write(filedata)
                FILE.flush()
        else:
            from Acquire.Client import PAR as _PAR
            par = _PAR.from_data(response["download_par"])
            par.read(privkey).get_object_as_file(downloaded_name)
            par.close(privkey)

            # validate that the size and checksum are correct
            filemeta.assert_correct_data(filename=downloaded_name)

            # uncompress the file if desired
            if filemeta.is_compressed():
                from Acquire.Client import uncompress as _uncompress
                _uncompress(inputfile=downloaded_name,
                            outputfile=downloaded_name,
                            compression_type=filemeta.compression_type())

        filemeta._set_drive(self)

        return (downloaded_name, filemeta)

    @staticmethod
    def _list_drives(user, drive_uid=None,
                     storage_service=None, storage_url=None):
        """Return a list of all of the DriveMetas of the drives accessible
           at the top-level by the passed user on the passed storage
           service

           Args:
                user (User): Name of file to download
                drive_uid (str, default=None): UID of drive
                storage_service (Service): Service for drives
                storage_url (str): URL for storage service          
           Returns:
                list: List of DriveMetas for the drives

        """
        if storage_service is None:
            storage_service = _get_storage_service(storage_url)
        else:
            if not storage_service.is_storage_service():
                raise TypeError("You can only query drives using "
                                "a valid storage service")

        from Acquire.Client import Authorisation as _Authorisation
        authorisation = _Authorisation(resource="UserDrives", user=user)

        args = {"authorisation": authorisation.to_data()}

        if drive_uid is not None:
            args["drive_uid"] = str(drive_uid)

        response = storage_service.call_function(
                                    function="list_drives", args=args)

        from Acquire.ObjectStore import string_to_list as _string_to_list
        from Acquire.Client import DriveMeta as _DriveMeta

        return _string_to_list(response["drives"], _DriveMeta)

    @staticmethod
    def list_toplevel_drives(user, storage_service=None, storage_url=None):
        """Return a list of all of the DriveMetas of the drives accessible
           at the top-level by the passed user on the passed storage
           service

           Args:
                user (User): User for drives
                storage_service (Service, default=None): Storage service to query
                storage_url (str): URL for storage service
           Returns:
                list: List of DriveMetas for the drives
        """
        return Drive._list_drives(user=user,
                                  storage_service=storage_service,
                                  storage_url=storage_url)

    def list_drives(self):
        """Return a list of the DriveMetas of all of the drives contained
           in this drive that are accessible to the user

           Returns:
                list: List of DriveMetas for the drives
        """
        if self.is_null():
            return []
        else:
            return Drive._list_drives(user=self._user,
                                      drive_uid=self._drive_uid,
                                      storage_service=self._storage_service)

    def list_files(self, include_metadata=False):
        """Return a list of the FileMetas of all of the files contained
           in this drive

           Args:
                include_metadata (bool, default=False): If True include
                metadata for the returned files
           Returns:
                list: List of FileMetas for files in drive
        """
        if self.is_null():
            return []

        from Acquire.Client import Authorisation as _Authorisation
        from Acquire.ObjectStore import string_to_list as _string_to_list
        from Acquire.Storage import FileMeta as _FileMeta

        if include_metadata:
            include_metadata = True
        else:
            include_metadata = False

        authorisation = _Authorisation(resource="list_files",
                                       user=self._user)

        args = {"authorisation": authorisation.to_data(),
                "drive_uid": self._drive_uid,
                "include_metadata": include_metadata}

        response = self.storage_service().call_function(function="list_files",
                                                        args=args)

        files = _string_to_list(response["files"], _FileMeta)

        for f in files:
            f._set_drive(self)

        return files

    def list_versions(self, filename, include_metadata=False):
        """Return a list of all of the versions of the specified file.
           This returns an empty list if there are no versions of this
           file

           Args:
                filename (str): Filename for listing of versions
                include_metadata (bool, default=False): If True include
                metadata for the returned files
           Returns:
                list: List of FileMetas for versions of file
        """
        if self.is_null():
            return []

        if include_metadata:
            include_metadata = True
        else:
            include_metadata = False

        from Acquire.Client import Authorisation as _Authorisation

        authorisation = _Authorisation(resource="list_versions %s" % filename,
                                       user=self._user)

        args = {"authorisation": authorisation.to_data(),
                "drive_uid": self._drive_uid,
                "include_metadata": include_metadata,
                "filename": filename}

        response = self.storage_service().call_function(
                                                function="list_versions",
                                                args=args)

        from Acquire.ObjectStore import string_to_list \
            as _string_to_list
        from Acquire.Storage import FileMeta as _FileMeta

        versions = _string_to_list(response["versions"], _FileMeta)
        for version in versions:
            version._set_drive(self)

        return versions
