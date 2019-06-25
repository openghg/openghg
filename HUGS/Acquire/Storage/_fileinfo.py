
__all__ = ["FileInfo", "VersionInfo"]

_version_root = "storage/version"

_fileinfo_root = "storage/file"

_file_root = "storage/file"


class VersionInfo:
    """This class holds specific info about a version of a file"""
    def __init__(self, filesize=None, checksum=None,
                 aclrules=None, is_chunked=False, compression=None,
                 identifiers=None):
        """Construct the version of the file that has the passed
           size and checksum, was uploaded by the specified user,
           and that has the specified aclrules, and whether or not
           this file is stored and transmitted in a compressed
           state
        """
        if is_chunked:
            from Acquire.ObjectStore import create_uid as _create_uid
            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.Storage import ACLRules as _ACLRules

            try:
                user_guid = identifiers["user_guid"]
            except:
                user_guid = None

            if user_guid is None:
                raise PermissionError(
                    "You must specify the user_guid of the user who is "
                    "uploading this version of the file!")

            if aclrules is None:
                from Acquire.Identity import ACLRules as _ACLRules
                aclrules = _ACLRules.inherit()
            else:
                if not isinstance(aclrules, _ACLRules):
                    raise TypeError("The aclrules must be type ACLRules")

            self._filesize = 0
            self._nchunks = 0
            self._checksum = None
            self._compression = None
            self._datetime = _get_datetime_now()
            self._file_uid = "%s/%s" % (_datetime_to_string(self._datetime),
                                        _create_uid(short_uid=True))
            self._user_guid = str(user_guid)
            self._aclrules = aclrules

        elif filesize is not None:
            from Acquire.ObjectStore import create_uid as _create_uid
            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.Storage import ACLRules as _ACLRules

            try:
                user_guid = identifiers["user_guid"]
            except:
                user_guid = None

            if user_guid is None:
                raise PermissionError(
                    "You must specify the user_guid of the user who is "
                    "uploading this version of the file!")

            if aclrules is None:
                from Acquire.Identity import ACLRules as _ACLRules
                aclrules = _ACLRules.inherit()
            else:
                if not isinstance(aclrules, _ACLRules):
                    raise TypeError("The aclrules must be type ACLRules")

            self._filesize = filesize
            self._checksum = checksum
            self._datetime = _get_datetime_now()
            self._file_uid = "%s/%s" % (_datetime_to_string(self._datetime),
                                        _create_uid(short_uid=True))
            self._user_guid = str(user_guid)
            self._compression = compression
            self._aclrules = aclrules
            self._nchunks = None

        else:
            self._filesize = None
            self._nchunks = None

    def is_null(self):
        """Return whether or not this is null"""
        return self._filesize is None

    def filesize(self):
        """Return the size in bytes of this version of the file"""
        if self.is_null():
            return 0
        else:
            return self._filesize

    def checksum(self):
        """Return the checksum for this version of the file"""
        if self.is_null():
            return None
        else:
            return self._checksum

    def is_chunked(self):
        """Return whether or not this file is chunked"""
        if self.is_null():
            return False
        else:
            return self._nchunks is not None

    def is_uploading(self):
        """Return whether we are still in the process of
           uploading the chunked file
        """
        if self.is_chunked():
            return self._checksum is None
        else:
            return False

    def close_uploader(self, file_bucket):
        """Close the uploader. This will count the number of chunks,
           and will also create a checksum of all of the chunk's
           checksums
        """
        if not self.is_uploading():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        meta_root = "%s/meta/" % self._file_key()

        keys = _ObjectStore.get_all_object_names(bucket=file_bucket,
                                                 prefix=meta_root)

        meta_keys = {}
        for key in keys:
            idx = int(key.split("/")[-1])
            meta_keys[idx] = key

        nchunks = len(keys)
        size = 0
        from Acquire.Crypto import Hash as _Hash
        from hashlib import md5 as _md5
        md5 = _md5()

        for i in range(0, nchunks):
            key = meta_keys[i]
            meta = _ObjectStore.get_object_from_json(
                            bucket=file_bucket, key="%s/%d" % (meta_root, i))

            size += meta["filesize"]
            md5.update(meta["checksum"].encode("utf-8"))

        self._filesize = size
        self._checksum = md5.hexdigest()
        self._nchunks = nchunks

    def num_chunks(self):
        """Return the number of chunks used for this file. This is
           equal to 1 for unchunked files, or for files that
           have only uploaded one chunk. This is zero for files
           that are still in the process of being uploaded
        """
        if self.is_null():
            return 0
        elif self.is_chunked():
            return self._nchunks
        else:
            return 1

    def aclrules(self):
        """Return all of the ACL rules for this version of the file"""
        if self.is_null():
            return None
        else:
            return self._aclrules

    def uid(self):
        """Return the UID of this version of the file in object store"""
        if self.is_null():
            return None
        else:
            return self._file_uid

    def is_compressed(self):
        """Return whether or not this file is stored and transmitted
           in a compressed state
        """
        if self.is_null():
            return False
        else:
            return self._compression is not None

    def compression_type(self):
        """Return the type of compression used if this file is
           stored and transmitted in a compressed state, or None
           if this is not compressed
        """
        if self.is_null():
            return None
        else:
            return self._compression

    def datetime(self):
        """Return the datetime when this version was created"""
        if self.is_null():
            return None
        else:
            return self._datetime

    def uploaded_by(self):
        """Return the GUID of the user that uploaded this version"""
        if self.is_null():
            return None
        else:
            return self._user_guid

    def _file_key(self):
        """Return the key for this actual file for this version
           in the object store. If this is a chunked file, then
           the data will be stored in objects as a sub-key of this
           key
        """
        if self.is_null():
            return None
        else:
            return "%s/%s" % (_file_root, self._file_uid)

    def _key(self, drive_uid, encoded_filename):
        """Return the key for this version in the object store"""
        if self.is_null():
            return None
        else:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            return "%s/%s/%s/%s" % (
                _version_root, drive_uid, encoded_filename, self._file_uid)

    def to_data(self):
        """Return a json-serialisable dictionary for this object"""
        data = {}

        if not self.is_null():
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.ObjectStore import dict_to_string \
                as _dict_to_string
            data["filesize"] = self._filesize
            data["checksum"] = self._checksum
            data["file_uid"] = self._file_uid
            data["user_guid"] = self._user_guid

            if self._nchunks is not None:
                data["nchunks"] = int(self._nchunks)

            if self._aclrules is not None:
                data["aclrules"] = self._aclrules.to_data()

            if self._compression is not None:
                data["compression"] = self._compression

        return data

    @staticmethod
    def from_data(data):
        """Return this object constructed from the passed json-deserialised
           dictionary
        """
        v = VersionInfo()

        if data is not None and len(data) > 0:
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime
            v._filesize = data["filesize"]
            v._checksum = data["checksum"]
            v._file_uid = data["file_uid"]
            v._user_guid = data["user_guid"]

            try:
                v._datetime = _string_to_datetime(data["datetime"])
                v._file_uid = "%s/%s" % (data["datetime"], v._file_uid)
            except:
                v._datetime = _string_to_datetime(v._file_uid.split("/")[0])

            if "aclrules" in data:
                from Acquire.Storage import ACLRules as _ACLRules
                v._aclrules = _ACLRules.from_data(data["aclrules"])

            if "compression" in data:
                v._compression = data["compression"]
            else:
                v._compression = None

            if "nchunks" in data:
                v._nchunks = int(data["nchunks"])
            else:
                v._nchunks = None

        return v


class FileInfo:
    """This class provides information about a user-file that has
       been uploaded to the storage service. This includes all
       versions of the file, the ACLs for different users etc.

       While Acquire.Client.Drive provides a client-side view of
       Acquire.Storage.DriveInfo, there is no equivalent client-side
       view of Acquire.Storage.FileInfo. This is because we operate
       on files via their drives.

       The metadata of a FileInfo is presented to the user via
       the Acquire.Client.FileMeta class (same way that DriveInfo
       metadata is presented to the user via the Acquire.Client.DriveMeta
       class)
    """
    def __init__(self, drive_uid=None, filehandle=None,
                 filename=None, aclrules=None, is_chunked=False,
                 identifiers=None, upstream=None):
        """Construct from a passed filehandle of a file that will be
           uploaded
        """
        self._filename = None

        if is_chunked:
            if filename is None or len(filename) == 0:
                raise TypeError(
                    "The filename must be a valid string")

            self._drive_uid = drive_uid

            from Acquire.ObjectStore import string_to_encoded \
                as _string_to_encoded
            from Acquire.ObjectStore import string_to_filepath \
                as _string_to_filepath

            filename = _string_to_filepath(filename)

            # remove any leading slashes
            while filename.startswith("/"):
                filename = filename[1:]

            # remove any trailing slashes
            while filename.endswith("/"):
                filename = filename[0:-1]

            self._filename = filename
            self._encoded_filename = _string_to_encoded(filename)

            version = VersionInfo(is_chunked=True,
                                  identifiers=identifiers,
                                  aclrules=aclrules)

            self._latest_version = version
            self._identifiers = identifiers
            self._upstream = upstream
        elif filehandle is not None:
            from Acquire.Storage import FileHandle as _FileHandle

            if not isinstance(filehandle, _FileHandle):
                raise TypeError(
                    "The filehandle must be of type FileHandle")

            if filehandle.is_null():
                return

            self._drive_uid = drive_uid

            from Acquire.ObjectStore import string_to_encoded \
                as _string_to_encoded
            from Acquire.ObjectStore import string_to_filepath \
                as _string_to_filepath

            filename = _string_to_filepath(filehandle.filename())

            # remove any leading slashes
            while filename.startswith("/"):
                filename = filename[1:]

            # remove any trailing slashes
            while filename.endswith("/"):
                filename = filename[0:-1]

            self._filename = filename
            self._encoded_filename = _string_to_encoded(filename)

            version = VersionInfo(filesize=filehandle.filesize(),
                                  checksum=filehandle.checksum(),
                                  identifiers=identifiers,
                                  compression=filehandle.compression_type(),
                                  aclrules=filehandle.aclrules())

            self._latest_version = version
            self._identifiers = identifiers
            self._upstream = upstream

    def is_null(self):
        """Return whether or not this is null"""
        return self._filename is None

    def filename(self):
        """Return the object-store filename for this file"""
        return self._filename

    @staticmethod
    def _get_filemeta(filename, version, identifiers, upstream):
        """Internal function used to create a FileMeta from the passed
           filename and VersionInfo object
        """
        from Acquire.Client import FileMeta as _FileMeta

        filemeta = _FileMeta(filename=filename, uid=version.uid(),
                             filesize=version.filesize(),
                             checksum=version.checksum(),
                             uploaded_by=version.uploaded_by(),
                             uploaded_when=version.datetime(),
                             compression=version.compression_type(),
                             aclrules=version.aclrules())

        filemeta.resolve_acl(identifiers=identifiers,
                             upstream=upstream,
                             must_resolve=True,
                             unresolved=False)

        return filemeta

    def get_filemeta(self, version=None):
        """Return the metadata about the latest (or specified) version
           of this file.
        """
        from Acquire.Client import FileMeta as _FileMeta

        if self.is_null():
            return _FileMeta()

        return FileInfo._get_filemeta(filename=self._filename,
                                      version=self._version_info(version),
                                      identifiers=self._identifiers,
                                      upstream=self._upstream)

    def _version_info(self, version=None):
        """Return the version info object of the latest version of
           the file, or the passed version
        """
        if self.is_null():
            return VersionInfo()
        elif version is None:
            return self._latest_version
        elif self._latest_version.uid() == version:
            return self._latest_version

        # lookup this version in the object store
        from Acquire.Storage import MissingVersionError
        raise MissingVersionError(
            "Cannot find the version '%s' for file '%s'" %
            (version, self.filename()))

    def filesize(self, version=None):
        """Return the size (in bytes) of the latest (or specified)
           version of this file"""
        return self._version_info(version=version).filesize()

    def checksum(self, version=None):
        """Return the checksum of the latest (or specified) version
           of this file
        """
        return self._version_info(version=version).checksum()

    def is_compressed(self, version=None):
        """Return whether or not the latest (or specified) version
           of this file is stored and transmitted in a compressed
           state
        """
        return self._version_info(version=version).is_compressed()

    def compression_type(self, version=None):
        """Return the compression type (or None if not compressed)
           of the latest (or specified) version of this file
        """
        return self._version_info(version=version).compression_type()

    def drive_uid(self):
        """Return the UID of the drive on which this file resides"""
        return self._drive_uid

    def drive(self):
        """Return the actual DriveInfo object for the drive on which this
           file resides
        """
        if self.is_null():
            return None
        else:
            from Acquire.Storage import DriveInfo as _DriveInfo
            return _DriveInfo(drive_uid=self.drive_uid())

    def file_uid(self, version=None):
        """Return the UID of the latest (or specified) version
           of this file
        """
        return self._version_info(version=version).uid()

    def aclrules(self, version=None):
        """Return the ACL rules for the specified user, or if that is not
           specified, the ACL rules for the current version
        """
        return self._version_info(version=version).aclrules()

    def version(self, version=None):
        """Return the version at the specified datetime. If nothing is
           specified, then return the version as loaded
        """
        if version is None:
            return self._latest_version
        else:
            return self._version_info(version=version)

    def latest_version(self):
        """Return the latest version of this file on the storage service. This
           is a datetime of the upload of the latest version. You will need to
           use the 'versions' function to find if there are other versions.
        """
        if self.is_null():
            return None
        else:
            return self._latest_version

    def versions(self):
        """Return the sorted list of all versions of this file on the
           storage service
        """
        if self.is_null():
            return []
        else:
            return {self._latest_version.datetime(), self._latest_version}

    def close_uploader(self, file_bucket):
        """Close the uploader"""
        if self.is_null():
            return
        elif not self._latest_version.is_uploading():
            return

        self._latest_version.close_uploader(file_bucket)

    def is_uploading(self):
        """Return whether this version is still in the process of
           being uploaded
        """
        return self._latest_version.is_uploading()

    def _fileinfo_key(self):
        """Return the key for this fileinfo in the object store"""
        return "%s/%s/%s" % (_fileinfo_root, self._drive_uid,
                             self._encoded_filename)

    def save(self):
        """Save this fileinfo to the object store"""
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        metadata_bucket = self.drive()._get_metadata_bucket()

        # save the version information (saves old versions)
        _ObjectStore.set_object_from_json(
                        bucket=metadata_bucket,
                        key=self._latest_version._key(self._drive_uid,
                                                      self._encoded_filename),
                        data=self._latest_version.to_data())

        # save the fileinfo itself
        _ObjectStore.set_object_from_json(bucket=metadata_bucket,
                                          key=self._fileinfo_key(),
                                          data=self.to_data())

    @staticmethod
    def list_versions(drive, filename, identifiers=None,
                      upstream=None, include_metadata=False):
        """List all of the versions of this file. If 'include_metadata'
           is True then this will load all of the associated metadata
           for each file
        """
        from Acquire.Storage import DriveInfo as _DriveInfo
        from Acquire.Storage import FileMeta as _FileMeta

        if not isinstance(drive, _DriveInfo):
            raise TypeError("The drive must be of type DriveInfo")

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        metadata_bucket = drive._get_metadata_bucket()

        encoded_filename = _string_to_encoded(filename)

        version_root = "%s/%s/%s/" % (
                _version_root, drive.uid(), encoded_filename)

        versions = []

        if include_metadata:
            objs = _ObjectStore.get_all_objects_from_json(
                                            bucket=metadata_bucket,
                                            prefix=version_root)

            for data in objs.values():
                version = VersionInfo.from_data(data)
                filemeta = FileInfo._get_filemeta(filename=filename,
                                                  version=version,
                                                  identifiers=identifiers,
                                                  upstream=upstream)

                if not filemeta.acl().denied_all():
                    versions.append(filemeta)
        else:
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime
            keys = _ObjectStore.get_all_object_names(
                                            bucket=metadata_bucket,
                                            prefix=version_root)

            for key in keys:
                parts = key.split("/")
                uploaded_when = _string_to_datetime(parts[-2])
                uid = "%s/%s" % (parts[-2], parts[-1])
                filemeta = _FileMeta(filename=filename,
                                     uploaded_when=uploaded_when,
                                     uid=uid)
                versions.append(filemeta)

        return versions

    @staticmethod
    def load(drive, filename, version=None, identifiers=None,
             upstream=None):
        """Load and return the FileInfo for the file called 'filename'
           on the passed 'drive'.
        """
        from Acquire.Storage import DriveInfo as _DriveInfo

        if not isinstance(drive, _DriveInfo):
            raise TypeError("The drive must be of type DriveInfo")

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        metadata_bucket = drive._get_metadata_bucket()

        encoded_filename = _string_to_encoded(filename)

        filekey = "%s/%s/%s" % (_fileinfo_root, drive.uid(),
                                encoded_filename)

        try:
            data = _ObjectStore.get_object_from_json(bucket=metadata_bucket,
                                                     key=filekey)
        except Exception as e:
            print(e)
            data = None

        if data is None:
            from Acquire.Storage import MissingFileError
            raise MissingFileError(
                "Cannot find the file called '%s' on drive '%s'" %
                (filename, drive))

        f = FileInfo.from_data(data)
        f._drive_uid = drive.uid()
        f._identifiers = identifiers
        f._upstream = upstream

        if version is not None:
            f._latest_version = f._version_info(version=version)

        return f

    def to_data(self):
        """Return a json-serialisable dictionary for this object"""
        data = {}

        if not self.is_null():
            data["filename"] = self.filename()
            data["latest_version"] = self.latest_version().to_data()

        return data

    @staticmethod
    def from_data(data, identifiers=None, upstream=None):
        """Return this object constructed from the passed json-deserialised
           dictionary. If 'identifier' and 'upstream' are passed
           then these set the user identifiers and upstream ACL
           of the file object as it was opened.
        """
        f = FileInfo()

        if data is not None and len(data) > 0:
            from Acquire.ObjectStore import string_to_encoded \
                as _string_to_encoded
            f._filename = data["filename"]
            f._encoded_filename = _string_to_encoded(f._filename)
            f._latest_version = VersionInfo.from_data(data["latest_version"])
            f._drive_uid = None
            f._identifiers = identifiers
            f._upstream = upstream

        return f
