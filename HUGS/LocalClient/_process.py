# The local version of the Process object
from pathlib import Path

from HUGS.Processing import DataTypes
from HUGS.Util import load_object

__all__ = ["Process"]


class Process:
    """ Process data files and store in the object store

    """

    def __init__(self):
        pass

    def process_folder(self, folder_path, data_type, overwrite=False, extension="dat"):
        """ Process the passed directory of data files

            Note: this does function does not recursively find files.

            Args:
                folder_path (str, pathlib.Path): Path of folder containing files to be processed
                data_type (str): Type of data to be processed (CRDS, GC etc)
                This may be removed in the future.
                storage_url (str): URL of storage service. Currently used for testing
                This may be removed in the future.
        """
        data_type = data_type.upper()

        if data_type == "GC":
            filepaths = []
            # Find all files in
            for f in Path(folder_path).glob("*.C"):
                if "precisions" in f.name:
                    # Remove precisions section and ensure the matching data file exists
                    data_filename = str(f).replace(".precisions", "")
                    if Path(data_filename).exists():
                        filepaths.append((Path(data_filename), f))
        else:
            filepaths = [f for f in Path(folder_path).glob(f"**/*.{extension}")]

     def process_files(self, files, site, instrument, network, data_type, overwrite=False):
        """ Process the passed file(s)

            Args:
                files (str, list): Path of files to be processed
                site (str): Site code or name
                instrument (str): Instrument name
                data_type (str): Type of data to be processed (CRDS, GC etc)
                overwrite (bool, default=False): Should this data overwrite data
                stored for these datasources for existing dateranges
            Returns:
                dict: UUIDs of Datasources storing data of processed files keyed by filename
        """
        data_type = DataTypes[data_type.upper()].name

        if not isinstance(files, list):
            files = [files]

        # Ensure we have Paths
        if data_type.upper() == "GC":
            if not all(isinstance(item, tuple) for item in files):
                return TypeError("If data type is GC, a list of tuples for data and precision filenames must be passed")

            files = [(Path(f), Path(p)) for f, p in files]
        else:
            files = [Path(f) for f in files]

        # Load in the the class used to process the data file/s
        processing_obj = load_object(class_name=data_type)
