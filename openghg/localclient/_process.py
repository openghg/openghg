# The local version of the Process object
from pathlib import Path
from typing import Dict, List, Union
from openghg.store import ObsSurface
from openghg.retrieve import DataTypes

__all__ = ["process_files"]


def process_files(
    files: Union[str, List],
    data_type: str,
    site: str,
    network: str,
    inlet: str = None,
    instrument: str = None,
    overwrite: bool = False,
) -> Dict:
    """Process the passed file(s)

    Args:
        files: Path of files to be processed
        data_type: Type of data to be processed (CRDS, GC etc)
        site: Site code or name
        network: Network name
        instrument: Instrument name
        overwrite: Should this data overwrite data
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
            raise TypeError("If data type is GC, a list of tuples for data and precision filenames must be passed")
        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    r = obs.read_file(
        filepath=files, data_type=data_type, site=site, network=network, instrument=instrument, inlet=inlet, overwrite=overwrite
    )
    results.update(r)

    return results
