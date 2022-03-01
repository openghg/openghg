from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union

from openghg.store import ObsSurface, Emissions, Footprints
from openghg.types import DataTypes
from openghg.util import running_in_cloud

obsFilepathType = Union[str, Path, List, Tuple[Path, Path]]
pathType = Union[str, Path]


def process_obs(
    files: obsFilepathType,
    data_type: str,
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    overwrite: bool = False,
) -> Dict:
    """Process observation files, standardise them and add the data to the object store

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
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return _process_obs_local(
            files=files,
            data_type=data_type,
            site=site,
            network=network,
            inlet=inlet,
            instrument=instrument,
            overwrite=overwrite,
        )


def process_flux(
    files: Union[str, Path],
    species: str,
    source: str,
    domain: str,
    date: str,
    high_time_resolution: Optional[bool] = False,
    period: Optional[str] = None,
    overwrite: bool = False,
) -> Dict:
    """Process flux data

    Args:
        filepath: Path of emissions file
        species: Species name
        domain: Emissions domain
        source: Emissions source
        high_time_resolution: If this is a high resolution file
        period: Period of measurements, if not passed this is inferred from the time coords
        overwrite: Should this data overwrite currently stored data.
    returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return _process_flux_local(
            files=files,
            species=species,
            source=source,
            domain=domain,
            date=date,
            high_time_resolution=high_time_resolution,
            period=period,
            overwrite=overwrite,
        )


def process_footprint(
    files: pathType,
    site: str,
    height: str,
    domain: str,
    model: str,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    retrieve_met: bool = False,
    overwrite: bool = False,
    high_res: bool = False,
) -> Dict:
    """Reads footprint data files and returns the UUIDs of the Datasources
    the processed data has been assigned to

    Args:
        filepath: Path of file to load
        site: Site name
        network: Network name
        height: Height above ground level in metres
        domain: Domain of footprints
        model_params: Model run parameters
        retrieve_met: Whether to also download meterological data for this footprints area
        overwrite: Overwrite any currently stored data
    Returns:
        dict: UUIDs of Datasources data has been assigned to
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return _process_footprint_local(
            filepath=files,
            site=site,
            height=height,
            domain=domain,
            model=model,
            metmodel=metmodel,
            species=species,
            network=network,
            retrieve_met=retrieve_met,
            overwrite=overwrite,
            high_res=high_res,
        )


def _process_obs_local(
    files: obsFilepathType,
    data_type: str,
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    overwrite: bool = False,
) -> Dict:
    """Process the passed observations file(s)

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

    results = {}
    # Ensure we have Paths
    # TODO: Delete this, as we already have the same warning in read_file?
    if data_type == "GCWERKS":
        if not all(isinstance(item, tuple) for item in files):
            raise TypeError(
                "If data type is GC, a list of tuples for data and precision filenames must be passed"
            )
        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    res = ObsSurface.read_file(
        filepath=files,
        data_type=data_type,
        site=site,
        network=network,
        instrument=instrument,
        inlet=inlet,
        overwrite=overwrite,
    )

    results.update(res)

    return results


def _process_footprint_local(
    filepath: pathType,
    site: str,
    height: str,
    domain: str,
    model: str,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    retrieve_met: bool = False,
    overwrite: bool = False,
    high_res: bool = False,
) -> Dict:
    """Reads footprints data files and returns the UUIDs of the Datasources
    the processed data has been assigned to in the local object store.

    Args:
        filepath: Path of file to load
        site: Site name
        network: Network name
        height: Height above ground level in metres
        domain: Domain of footprints
        model_params: Model run parameters
        retrieve_met: Whether to also download meterological data for this footprints area
        overwrite: Overwrite any currently stored data
    Returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    return Footprints.read_file(
        filepath=filepath,
        site=site,
        height=height,
        domain=domain,
        model=model,
        metmodel=metmodel,
        species=species,
        network=network,
        retrieve_met=retrieve_met,
        overwrite=overwrite,
        high_res=high_res,
    )


def _process_flux_local(
    files: pathType,
    species: str,
    source: str,
    domain: str,
    date: str,
    high_time_resolution: Optional[bool] = False,
    period: Optional[str] = None,
    overwrite: bool = False,
) -> Dict:
    """Process flux data for the local object store

    Args:
        filepath: Path of emissions file
        species: Species name
        domain: Emissions domain
        source: Emissions source
        high_time_resolution: If this is a high resolution file
        period: Period of measurements, if not passed this is inferred from the time coords
        overwrite: Should this data overwrite currently stored data.
    returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """

    return Emissions.read_file(
        filepath=files,
        species=species,
        source=source,
        domain=domain,
        date=date,
        high_time_resolution=high_time_resolution,
        period=period,
        overwrite=overwrite,
    )
