# The local version of the Process object
from pathlib import Path

from HUGS.Modules import ObsSurface
from HUGS.Processing import DataTypes

__all__ = ["process_folder", "process_files"]


def process_folder(folder_path, data_type, overwrite=False, extension="dat"):
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

    return process_files(files=filepaths, data_type=data_type)


def process_files(files, data_type, site=None, network=None, instrument=None, overwrite=False):
    """ Process the passed file(s)

        Args:
            files (str, list): Path of files to be processed
            data_type (str): Type of data to be processed (CRDS, GC etc)
            site (str, default=None): Site code or name
            network (str, default=None): Network name
            instrument (str, default=None): Instrument name
            overwrite (bool, default=False): Should this data overwrite data
            stored for these datasources for existing dateranges
        Returns:
            dict: UUIDs of Datasources storing data of processed files keyed by filename
    """
    data_type = DataTypes[data_type.upper()].name

    if not isinstance(files, list):
        files = [files]

    obs = ObsSurface.load()

    results = {}
    # Ensure we have Paths
    if data_type == "GC":
        if not all(isinstance(item, tuple) for item in files):
            return TypeError("If data type is GC, a list of tuples for data and precision filenames must be passed")
        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    r = obs.read_file(filepath=files, data_type=data_type, site=site, network=network, instrument=instrument)
    results.update(r)

    return results
