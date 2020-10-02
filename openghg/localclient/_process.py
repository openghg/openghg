# The local version of the Process object
from pathlib import Path

from openghg.modules import ObsSurface
from openghg.processing import DataTypes

__all__ = ["process_files"]


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
    # TODO: Delete this, as we already have the same warning in read_file?
    if data_type == "GCWERKS":
        if not all(isinstance(item, tuple) for item in files):
            return TypeError("If data type is GC, a list of tuples for data and precision filenames must be passed")
        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    r = obs.read_file(filepath=files, data_type=data_type, site=site, network=network, instrument=instrument, overwrite=overwrite)
    results.update(r)

    return results
