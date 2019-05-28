"""
Modules that separates out all of the code to run a gromacs
simulation away from the code used to interface with Fn

@author Christopher Woods
"""

import os as _os
import glob as _glob
import subprocess as _subprocess
import datetime as _datetime
import tarfile as _tarfile
import tempfile as _tempfile

from ._objstore import ObjectStore as objstore

try:
    from watchdog.observers import Observer as _Observer
    from watchdog.events import FileSystemEventHandler as _FileSystemEventHandler
    _have_watchdog = True
except:
    _have_watchdog = False

__all__ = [ "Error", "GromppError", "MDRunError", "GromacsRunner" ]

class Error(Exception):
    """Base class for exceptions in this module"""
    pass

class GromppError(Error):
    """Exception caused by failure of grompp"""
    pass

class MDRunError(Error):
    """Exception raised by a failure of mdrun"""
    pass

class _FileWatcher:
    """This class is used to watch a specific file,
       uploading chunks of the file to an object store
       when the watcher is updated"""
    def __init__(self, filename, bucket, rootkey,
                 sizetrigger, timetrigger):
        self._filename = filename
        self._bucket = bucket
        self._rootkey = rootkey
        self._handle = None
        self._key = None
        self._buffer = None
        self._last_upload_time = _datetime.datetime.now()
        self._next_chunk = 0
        self._chunksize = 8192
        self._uploadsize = int(sizetrigger)
        self._upload_timeout = int(timetrigger)

    def _uploadBuffer(self):
        """Internal function that uploads the current buffer to
           a new chunk in the object store"""
        if self._buffer is None:
            return
        elif len(self._buffer) == 0:
            #nothing to upload
            return

        self._next_chunk += 1
        self._last_upload_time = _datetime.datetime.now()

        objstore.log(self._bucket, "Upload %s chunk (%f KB) to %s/%s" % \
                       (self._filename, float(len(self._buffer))/1024.0,
                        self._key, self._next_chunk))

        objstore.set_object(self._bucket,
                            "%s/%d" % (self._key,self._next_chunk),
                            self._buffer)

        self._buffer = None

    def finishUploads(self):
        """Finalise the uploads"""
        self._uploadBuffer()

    def update(self, force_upload=False):
        """Called whenever the file changes"""
        if not self._key:
            # the file hasn't been opened or uploaded yet - create
            # key for this file, open the file and read as much as
            # possible into a buffer, ready for upload
            if self._rootkey:
                self._key = "%s/%s" % (self._rootkey,self._filename)
            else:
                self._key = self._filename

            # open the file and connect to the filehandle
            self._handle = open(self._filename, "rb")

        # read in the next chunk of the file
        while True:
            chunk = self._handle.read(self._chunksize)

            if chunk:
                if not self._buffer:
                    self._buffer = chunk
                else:
                    self._buffer += chunk

                if len(self._buffer) > self._uploadsize:
                    self._uploadBuffer()
            else:
                # nothing more to read
                break
    
        # we have read in everything that has been produced - should 
        # we upload it? Only upload if more than 5 seconds have passed
        # since the last update
        if force_upload:
            try:
                bufsize = len(self._buffer)
            except:
                bufsize = 0

            if bufsize > 0:
                objstore.log(self._bucket, "Uploading last of %s (%d bytes)" % \
                                    (self._filename, 0))

                self._uploadBuffer()

        elif (_datetime.datetime.now() - self._last_upload_time).seconds \
                  > self._upload_timeout:
            self._uploadBuffer()
        
if _have_watchdog:
    class _PosixToObjstoreEventHandler(_FileSystemEventHandler):
        """This class responds to events in the filesystem. 
           The aim is to detect as files are created and modified,
           and to stream this data up to the object store while
           the simulation is in progress. This is called in 
           a background thread by watchdog"""

        def __init__(self, bucket, rootkey=None,
                     sizetrigger=8*1024*1024, timetrigger=5):
            _FileSystemEventHandler.__init__(self)
            self._bucket = bucket
            self._rootkey = rootkey
            self._sizetrigger = int(sizetrigger)
            self._timetrigger = int(timetrigger)
            self._files = {}

        def chunkSizeTrigger(self):
            """Return the size of buffer that will trigger a write to 
               the object store"""
            return self._sizetrigger

        def chunkTimeTrigger(self):
            """Return the amount of time between writes that will trigger
               a write to the object store"""
            return self._timetrigger

        def finishUploads(self):
            """Call this to complete all of the uploads"""
            for f in self._files:
                self._files[f].finishUploads()

        def on_any_event(self, event):
            """This function is called on any filesystem event. If locates
               the changed file and reads the file into a buffer. This is
               uploaded to the object store if one of two conditions are
               met:
                1. The amount of data written exceeds self.chunkSizeTrigger()
                2. More than self.chunkTimeTrigger() seconds has passsed
            """
            if event.is_directory:
                return

            filename = event.src_path

            if filename.startswith("./"):
                filename = filename[2:]

            if not filename in self._files:
                self._files[filename] = _FileWatcher(filename, bucket=self._bucket, 
                                                     rootkey=self._rootkey,
                                                     sizetrigger=self.chunkSizeTrigger(),
                                                     timetrigger=self.chunkTimeTrigger())

            self._files[filename].update()

        def finaliseUploads(self):
            """Ensure that the last parts of any files are uploaded
               before this observer exits"""
            objstore.log(self._bucket, "Finalising upload...")

            for filename in self._files:
                self._files[filename].update(True)

            self.finishUploads()

else:
    class _PosixToObjstoreEventHandler:
        def __init__(self, **kwargs):
            raise Error("Cannot follow files without watchdog!")

class GromacsRunner:
    @staticmethod
    def run(bucket):
        """Run the gromacs simulation whose input is contained
           in the passed bucket. Read the input from /input, 
           write a log to /log and write the output to /output
        """

        # path to the gromacs executables
        gmx = "/usr/local/gromacs/bin/gmx"

        # Clear the log for this simulation
        objstore.clear_log(bucket)

        # create a log function for logging messages to this bucket
        log = lambda message: objstore.log(bucket,message)

        # create a set_status function for setting the simulation status
        set_status = lambda status: objstore.set_string_object(bucket, "status", status)

        set_status("Loading...")

        # create a temporary directory for the simulation
        # (this ensures we are in the writable part of the container)
        tmpdir = _tempfile.mkdtemp()

        _os.chdir(tmpdir)

        log("Running a gromacs simulation in %s" % tmpdir)

        # get the value of the input key
        input_tar_bz2 = objstore.get_object_as_file(bucket, "input.tar.bz2", 
                                                    "/%s/input.tar.bz2" % tmpdir)

        # now unpack this file
        with _tarfile.open(input_tar_bz2, "r:bz2") as tar:
            tar.extractall(".")

        # remove the tar file to save space
        _os.remove(input_tar_bz2)

        # all simulation will take place in the "output" directory
        _os.makedirs("output")
        _os.chdir("output")

        # get the name of the mdp, top and gro files
        mdpfile = _glob.glob("../*.mdp")[0]
        topfile = _glob.glob("../*.top")[0]
        grofile = _glob.glob("../*.gro")[0]

        set_status("Preparing...")

        # run grompp to generate the input
        cmd = "%s grompp -f %s -c %s -p %s -o run.tpr" % (gmx,mdpfile,grofile,topfile)
        log("Running '%s'" % cmd)

        grompp_stdout = open("grompp.out", "w")
        grompp_stderr = open("grompp.err", "w")

        status = _subprocess.run([gmx, "grompp",
                                 "-f", mdpfile,
                                 "-c", grofile,
                                 "-p", topfile,
                                 "-o", "run.tpr"],
                                 stdout=grompp_stdout, stderr=grompp_stderr)

        log("gmx grompp completed. Return code == %s" % status.returncode)

        # Upload the grompp output to the object store
        objstore.set_object_from_file(bucket, "output/grompp.out", "grompp.out")
        objstore.set_object_from_file(bucket, "output/grompp.err", "grompp.err")

        if status.returncode != 0:
            raise GromppError("Grompp failed to run: Error code = %s" % 
                              status.returncode)

        # now write a run script to run the process and output the result
        cmd = "%s mdrun -v -deffnm run > mdrun.stdout 2> mdrun.stderr" % gmx
        FILE = open("run_mdrun.sh", "w")
        FILE.write("#!/bin/bash\n")
        FILE.write("%s\n" % cmd)
        FILE.close()

        set_status("Running...")

        # Start a watchdog process to look for new files
        if _have_watchdog:
            observer = _Observer()
            event_handler = _PosixToObjstoreEventHandler(bucket,
                                                         rootkey="interim",
                                                         timetrigger=1)

            observer.schedule(event_handler, ".", recursive=False)

            log("Starting the filesystem observer...")
            observer.start()

        # start the processor in the background
        log("Running '%s'" % cmd)
        PROC = _os.popen("bash run_mdrun.sh", "r")

        # wait for the gromacs job to finish...
        status = PROC.close()

        log("Gromacs has finished. Waiting for filesystem observer...")

        if _have_watchdog:
            # stop monitoring for events
            observer.stop()
            observer.join()
            event_handler.finaliseUploads()

        log("gmx mdrun completed. Return code == %s" % status)

        set_status("Uploading output...")

        # Upload all of the output files to the output directory
        log("Uploading mdrun.stdout")
        objstore.set_object_from_file(bucket, "output/mdrun.stdout", "mdrun.stdout")
        log("Uploading mdrun.stderr")
        objstore.set_object_from_file(bucket, "output/mdrun.stderr", "mdrun.stderr")

        for filename in _glob.glob("run.*"):
            if not filename.endswith("tpr"):
                log("Uploading %s" % filename)
                objstore.set_object_from_file(bucket,
                                              "output/%s" % filename, filename) 

        log("Simulation and data upload complete.")

        if status:
            set_status("Error")
            return (status, "Simulation finished with ERROR")
        else:
            set_status("Completed")
            return (0, "Simulation finished successfully")
