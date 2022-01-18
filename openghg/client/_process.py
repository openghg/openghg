from pathlib import Path
from typing import Dict, List, Union

from Acquire.Client import (
    Authorisation,
    Drive,
    PAR,
    Service,
    StorageCreds,
    User,
    Wallet,
)
from openghg.store import ObsSurface
from openghg.types import DataTypes


def process_files(
    files: Union[str, List],
    data_type: str,
    site: str,
    network: str,
    inlet: str = None,
    instrument: str = None,
    overwrite: bool = False,
    service_url: str = "https://fn.openghg.org/t",
    user: User = None,
) -> Dict:
    """Process data files, standardise them and add the data to the object store

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
    cloud = False

    if cloud:
        return _process_files_cloud(
            files=files,
            data_type=data_type,
            site=site,
            network=network,
            inlet=inlet,
            instrument=instrument,
            overwrite=overwrite,
            service_url=service_url,
            user=user,
        )
    else:
        return _process_files_local(
            files=files,
            data_type=data_type,
            site=site,
            network=network,
            inlet=inlet,
            instrument=instrument,
            overwrite=overwrite,
        )


def _process_files_cloud(
    files: Union[str, List],
    data_type: str,
    site: str,
    network: str,
    inlet: str = None,
    instrument: str = None,
    overwrite: bool = False,
    service_url: str = "https://fn.openghg.org/t",
    user: User = None,
) -> Dict:
    """Process the passed file(s)

    Args:
        user: Authenticated Acquire User
        files (str, list): Path of files to be processed
        data_type: Type of data to be processed (CRDS, GCWERKS etc)
        site: Site name
        network: Network name
        instrument: If no instrument name is passed we will attempt
        to find it from the filename.
        site: Name of site, three letter code or long name
    Returns:
        dict: UUIDs of Datasources storing data of processed files keyed by filename
    """
    wallet = Wallet()
    cloud_service = wallet.get_service(service_url=f"{service_url}/openghg")

    data_type = data_type.upper()

    if not isinstance(files, list):
        files = [files]

    if data_type in ("GCWERKS", "GC"):
        if not all(isinstance(item, tuple) for item in files):
            raise TypeError(
                "If data type is GCWERKS, a tuple or list of tuples for data and precision filenames must be passed"
            )

        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    storage_url = f"{service_url}/storage"
    openghg_url = f"{service_url}/openghg"

    openghg = Service(service_url=openghg_url)
    creds = StorageCreds(user=user, service_url=storage_url)
    drive = Drive(creds=creds, name="openghg_drive")
    auth = Authorisation(resource="process", user=user)

    # TODO - this should also just upload all the files at once and get them processed
    results = {}
    for file in files:
        if data_type in ("GCWERKS", "GC"):

            if "-" in site:
                site = site.split("-")[0]

            filemeta = drive.upload(file[0])
            par = PAR(location=filemeta.location(), user=user)
            par_secret = openghg.encrypt_data(par.secret())

            prec_meta = drive.upload(file[1])
            prec_par = PAR(location=prec_meta.location(), user=user)
            prec_par_secret = openghg.encrypt_data(prec_par.secret())

            args = {
                "authorisation": auth.to_data(),
                "par": {"data": par.to_data(), "precision": prec_par.to_data()},
                "par_secret": {"data": par_secret, "precision": prec_par_secret},
            }
        else:
            filemeta = drive.upload(file)
            par = PAR(location=filemeta.location(), user=user)
            par_secret = openghg.encrypt_data(par.secret())

            args = {
                "authorisation": auth.to_data(),
                "par": {"data": par.to_data()},
                "par_secret": {"data": par_secret},
            }

        all_types = {
            "data_type": data_type,
            "overwrite": overwrite,
            "site": site,
            "network": network,
        }

        args.update(all_types)

        # If we try to upload many files we don't want it to fail if a single
        # file contains overlapping data
        response: Dict = cloud_service.call_function(function="process.process", args=args)

        if "Error" in response:
            if data_type in ("GCWERKS", "GC"):
                filename = file[0].name
            else:
                filename = file.name

            results[filename] = response["Error"]
        elif "results" in response:
            results.update(response["results"])

    return results


def _process_files_local(
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
            raise TypeError(
                "If data type is GC, a list of tuples for data and precision filenames must be passed"
            )
        files = [(Path(f), Path(p)) for f, p in files]
    else:
        files = [Path(f) for f in files]

    r = obs.read_file(
        filepath=files,
        data_type=data_type,
        site=site,
        network=network,
        instrument=instrument,
        inlet=inlet,
        overwrite=overwrite,
    )
    results.update(r)

    return results
