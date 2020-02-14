from datetime import datetime
from pathlib import Path
from Acquire.Client import PAR

class Drive:
    """" This is used to upload files to the cloud drive for use in a
    HPC job

        Args:
            PAR: An Acquire PAR (Pre-Authenticated Request) used to
            access the cloud drive
    """
    def __init__(self, PAR):
        self._drive = None
        self._PAR  = PAR
        self._drive = PAR.resolve()

    def upload(self, files, directory="input"):
        """ Upload files to the cloud drive that's accessed using the Acquire
            PAR

            Args:
                files (str, Path, list): File(s) to be uploaded
                directory (str, default="input"): Name of directory in which to place files
            Returns:
                None
        """
        if not isinstance(files, list):
            files = [files]

        for f in files:
            self._drive.upload(filename=f, dir=directory)

    def list_files(self):
        """ List files in drive

            Returns:
                list: List of files
        """
        return self._drive.list_files(include_metadata=True)

    def download(self, files, local_dir=None):
        """ Download the files in the given list to a local directory

            Args:
                files (list): File(s) on drive
                local_dir (Path, str): Path of folder to download files to
            Returns:
                dict: Dictionary keyed by filename containing file metadata
        """
        if not isinstance(files, list):
            files = [files]

        if local_dir is None:
            date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
            folder_name = f"drive_download_{date_str}"
            local_dir = Path.home().joinpath(folder_name)

        file_metadata = {}
        for fname in files:
            file_metadata[fname] = self._drive.download(filename=fname, dir=local_dir)

        return file_metadata
            


    






