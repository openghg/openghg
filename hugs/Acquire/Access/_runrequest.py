
import os as _os
import uuid as _uuid

from Acquire.Access import Request as _Request

__all__ = ["RunRequest"]


def _get_abspath_size_md5(basedir, key, filename, max_size=None):
    """Assert that the specified filename associated with key 'key' exists
       and is readable by this user. Assert also that the filesize if below
       'size' bytes, is 'max_size' has been specified. This returns the
       absolute filename path for the file, the size of the file in bytes
       and the md5 checksum of the file, as a tuple

       Args:
            basedir (str): directory in which to find file
            key (str): key for file
            filename (str): filename
            max_size (int, optional, default=None): maximum size
            of file to process
            
        Returns:
            tuple (str, int, str): filename, filesize in bytes, MD5 
            checksum of file

    """

    if _os.path.isabs(filename):
        filename = _os.path.realpath(filename)
    else:
        filename = _os.path.realpath(_os.path.join(basedir, filename))

    try:
        FILE = open(filename, "r")
        FILE.close()
    except Exception as e:
        from Acquire.Service import exception_to_string
        from Acquire.Access import RunRequestError
        raise RunRequestError(
            "Cannot complete the run request because the file '%s' is not "
            "readable: filename=%s.\n\nCAUSE: %s" %
            (key, filename, exception_to_string(e)))

    from Acquire.Access import get_filesize_and_checksum \
        as _get_filesize_and_checksum

    (filesize, md5) = _get_filesize_and_checksum(filename)

    if filesize > max_size:
        raise RunRequestError(
            "Cannot complete the run request because the file '%s' is "
            "too large: filename=%s, filesize=%f MB, max_size=%f MB" %
            (key, filename, filesize/(1024.0*1024.0),
             max_size/(1024.0*1024.0)))

    return (filename, filesize, md5)


class RunRequest(_Request):
    """This class holds a request to run a particular calculation
       on a RunService. The result of this request will be a
       PAR to which the input should be loaded, and a Bucket
       from which the output can be read. The calculation will
       start once the input has been signalled as loaded.
    """
    def __init__(self, runfile=None):
        """Construct the request
        """
        super().__init__()

        self._uid = None
        self._runinfo = None
        self._tarfile = None
        self._tarfilename = None
        self._tarsize = None
        self._tarmd5 = None

        if runfile is not None:
            # Package up the simulation described in runfile
            self._set_runfile(runfile)

    def is_null(self):
        """Return whether or not this is a null request
        
        Returns:
            bool: True if UID is set, else False
                
        """
        return self._uid is None

    def __str__(self):
        if self.is_null():
            return "RunRequest::null"
        else:
            return "RunRequest(uid=%s)" % self._uid

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def uid(self):
        """Return the UID of this request"""
        return self._uid

    def fingerprint(self):
        """Return a unique fingerprint for this request that can be
           used for signing and verifying authorisations

            Returns:
                None or str: If null returns None, else returns a string
                combining the UID, size of tarfile and tarfile MD5 checksum
                as a fingerprint
        """
        if self.is_null():
            return None

        return "%s%s%s" % (self.uid(), self.tarfile_size(),
                           self.tarfile_md5sum())

    def tarfile(self):
        """Return the name of the tarfile containing all of the
           input files

            Returns:
                str: Name of tarfile
        """
        return self._tarfilename

    def tarfile_size(self):
        """Return the size of the tarfile in bytes
        
            Returns:
                int: Size of tarfile in bytes
        """
        return self._tarsize

    def tarfile_md5sum(self):
        """Return the MD5 checksum of the tarfile containing
           the input files

            Returns:
                str: MD5 checksum of tarfile
           
        """
        return self._tarmd5

    def runinfo(self):
        """Return the processed run information used to describe the
           calculation to be run. This includes information about all
           of the input files, such as their names, filesizes and
           MD5 checksums

            Returns:
                dict: Dictionary containing information about
                input files, names, filesizes, MD5 checksums

        """
        import copy as _copy
        return _copy.deepcopy(self._runinfo)

    def input_files(self):
        """Return a dictionary of the input file information for the
           input files for the calculation. This is a dictionary mapping
           the key for each file to the filename in the tarfile, the
           size of the file in the tarfile and the md5 sum of the file

            Returns:
                dict or None: Dictionary of input file information if 
                available, else None
        """
        if self._runinfo is None:
            return None

        if "input" in self._runinfo:
            return self._runinfo["input"]
        else:
            return None

    def _validate_input(self, basedir, runinfo):
        """Validate that the passed input 'runinfo' is correct, given
           it was loaded from the directory 'basedir'. This
           makes sure that all of the input files exist and are readable
           relative to 'basedir'. These MUST be declared in the 'input'
           section of the dictionary. This returns an updated 'runinfo'
           which has all relative paths converted to absolute file paths

            Args:
                basedir (str): directory from which to load data
                runinfo (dict): information regarding files to be
                used

            Returns:
                dict: Dictionary of validated file information
                including their sizes and MD5 checksums

        """
        if "input" not in runinfo:
            return runinfo

        try:
            items = runinfo["input"][0].items()
        except:
            try:
                items = runinfo["input"].items()
            except:
                from Acquire.Access import RunRequestError
                raise RunRequestError(
                    "Cannot execute the request because the input files "
                    "are specified with the wrong format. They should be "
                    "a single dictionary of key-value pairs. "
                    "Instead it is '%s'" % str(runinfo["input"]))

        input = {}

        for (key, filename) in items:
            # check the file exists and is not more than 100 MB is size
            (absfile, filesize, md5) = _get_abspath_size_md5(
                                                basedir, key,
                                                filename,
                                                100*1024*1024)

            input[key] = (absfile, filesize, md5)

        runinfo["input"] = input

        return runinfo

    def _create_tarfile(self):
        """This function creates the new tarfile, records its
            size and MD5 checksum and updates the runinfo with
            the paths for the input files in the zipfile

            Returns:
                None

        """
        if self._tarfile is not None:
            from Acquire.Access import RunRequestError
            raise RunRequestError("You cannot create the tarfile twice...")

        if "input" not in self._runinfo:
            return

        input = self._runinfo["input"]

        import tarfile as _tarfile
        import tempfile as _tempfile

        # Loop through each file - add it to tar.bz2. The files are added
        # flat into the tar.bz2, i.e. with no subdirectory. This is to
        # prevent strange complications or clashes with other files that
        # the user may create during output (on the server the files will
        # be unpacked into a uniquely-named directory)
        names = {}

        tempfile = _tempfile.NamedTemporaryFile(suffix="tar.bz2")
        tarfile = _tarfile.TarFile(fileobj=tempfile, mode="w")

        for (key, fileinfo) in input.items():
            (filename, filesize, md5) = fileinfo

            name = _os.path.basename(filename)

            # make sure that there isn't a matching file in the tarfile
            i = 0
            while name in names:
                i += 1
                name = "%d_%s" % (i, name)

            tarfile.add(name=filename, arcname=name, recursive=False)

            input[key] = (name, filesize, md5)

        tarfile.close()

        # close the file so that it is written to the disk - if we close
        # the tempfile then the file is deleted... (which shouldn't happen
        # until the object is deleted)
        tempfile.file.close()

        self._tarfile = tempfile
        self._tarfilename = tempfile.name

        from Acquire.Access import get_filesize_and_checksum \
            as _get_filesize_and_checksum
        (filesize, md5) = _get_filesize_and_checksum(tempfile.name)

        self._tarsize = filesize
        self._tarmd5 = md5

    def _set_runfile(self, runfile):
        """Run the simulation described in the passed runfile (should
           be in yaml or json format). This gives the type of simulation, the
           location of the input files and how the output should be
           named

           Args:
                runfile (str): YAML or JSON format file to be used
                to run simulation
            Returns:
                None
        """
        if self._runinfo:
            from Acquire.Access import RunRequestError
            raise RunRequestError(
                "You cannot change runfile of this RunRequest")

        if runfile is None:
            return

        runlines = None

        try:
            with open(runfile, "r") as FILE:
                runlines = FILE.read()
        except Exception as e:
            from Acquire.Service import exception_to_string
            from Acquire.Access import RunRequestError
            raise RunRequestError(
                "Cannot read '%s'. You must supply a readable input file "
                "that describes the calculation to be performed and supplies "
                "the names of all of the input files.\n\nCAUSE: %s" %
                (runfile, exception_to_string(e)))

        # get the directory that contains this file
        basedir = _os.path.dirname(_os.path.abspath(runfile))

        # try to parse this input as yaml
        runinfo = None

        try:
            import yaml as _yaml
            runinfo = _yaml.safe_load(runlines)
        except:
            pass

        if runinfo is None:
            try:
                import json as _json
                runinfo = _json.loads(runlines)
            except:
                pass

        if runinfo is None:
            from Acquire.Access import RunRequestError
            raise RunRequestError(
                "Cannot interpret valid input read from the file '%s'. "
                "This should be in json or yaml format, and this parser "
                "be built with that support." % runfile)

        runinfo = self._validate_input(basedir, runinfo)
        self._runinfo = runinfo

        self._create_tarfile()

        # everything is ok - set the UID of this request
        self._uid = str(_uuid.uuid4())

    def to_data(self):
        """Return this request as a json-serialisable dictionary

            Returns:
                dict: JSON serialisable dictionary created from object
        
        """
        if self.is_null():
            return {}

        data = super().to_data()
        data["uid"] = self._uid
        data["runinfo"] = self._runinfo
        data["tarsize"] = self._tarsize
        data["tarmd5"] = self._tarmd5

        return data

    @staticmethod
    def from_data(data):
        """
            Creates a RunRequest object from the JSON data in data

            Args:
                data (str): JSON deserialisable string used to create object
            Returns:
                RunRequest or None: If data contains JSON data create
                RunRequest object, else return None

        """
        if (data and len(data) > 0):
            r = RunRequest()

            r._uid = data["uid"]
            r._runinfo = data["runinfo"]
            r._tarsize = int(data["tarsize"])
            r._tarmd5 = data["tarmd5"]

            r._from_data(data)

            return r

        return None
