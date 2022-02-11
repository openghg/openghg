# import os

# from Acquire.Client import PAR

# # type: ignore


# class JobDrive:
#     """ " This is used to upload files to the cloud drive for use in a
#     HPC job

#     Args:
#         par (Acquire.Client.PAR): Pre-authenticated request for access to cloud drive
#         par_secret (str): Secret / password to access the PAR
#     """

#     def __init__(self, par, par_secret=None):
#         if not isinstance(par, PAR):
#             raise TypeError("par argument must be of type Acquire.Client.PAR")

#         self._par = par
#         self._par_secret = par_secret
#         self._drive = par.resolve(secret=par_secret)

#     def upload(self, files, directory="input"):
#         """Upload files to the cloud drive that's accessed using the Acquire
#         PAR

#         Args:
#             files (str, Path, list): File(s) to be uploaded
#             directory (str, default="input"): Name of directory in which to place files
#         Returns:
#             None
#         """
#         if not isinstance(files, list):
#             files = [files]

#         # 50 MB in bytes
#         chunk_limit = 50 * 1024 * 1024

#         # We might not have any data files to upload
#         for f in files:
#             filesize = os.path.getsize(f)
#             if filesize < chunk_limit:
#                 self._drive.upload(filename=f, directory=directory)
#             else:
#                 self._drive.chunk_upload(filename=f, directory=directory)

#     def list_files(self):
#         """List files in drive

#         Returns:
#             list: List of files
#         """
#         return self._drive.list_files(include_metadata=True)

#     def download(self, files, local_dir=None):
#         """Download the files in the given list to a local directory

#         Args:
#             files (list): File(s) on drive
#             local_dir (Path, str): Path of folder to download files to
#         Returns:
#             dict: Dictionary keyed by filename containing file metadata
#         """
#         if not isinstance(files, list):
#             files = [files]

#         # if local_dir is None:
#         #     date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
#         #     folder_name = f"drive_download_{date_str}"
#         #     local_dir = Path.home().joinpath(folder_name).mkdir()

#         file_metadata = {}
#         for fname in files:
#             file_metadata[fname] = self._drive.download(filename=fname, directory=local_dir)

#         return file_metadata
