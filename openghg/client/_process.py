__all__ = ["Process"]

from Acquire.Client import Wallet
from Acquire.Client import Drive, Service, PAR, Authorisation, StorageCreds
from pathlib import Path
from typing import Dict, List, Union, Optional


class Process:
    """Upload data to the cloud platform for processing and storage
    in the object store.
    """

    def __init__(self, service_url: Optional[str] = None):
        """Process a datafile using the passed user account

        service_url = "https://fn.openghg.org/t"

        Args:
            service_url: URL of service
        """
        if service_url:
            self._service_url = service_url
        else:
            self._service_url = "https://fn.openghg.org/t"

        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

    def process_files(
        self,
        user,
        files: Union[str, List],
        data_type: str,
        site: str,
        network: str,
        instrument: Optional[str] = None,
        overwrite: Optional[bool] = False,
        openghg_url: Optional[str] = None,
        storage_url: Optional[str] = None,
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
            openghg_url: URL of OpenGHG service. Currently used for testing
            This may be removed in the future.
            storage_url: URL of storage service. Currently used for testing
            This may be removed in the future.
            site: Name of site, three letter code or long name
        Returns:
            dict: UUIDs of Datasources storing data of processed files keyed by filename
        """
        data_type = data_type.upper()

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        if not isinstance(files, list):
            files = [files]

        if data_type in ("GCWERKS", "GC"):
            if not all(isinstance(item, tuple) for item in files):
                raise TypeError("If data type is GCWERKS, a tuple or list of tuples for data and precision filenames must be passed")

            files = [(Path(f), Path(p)) for f, p in files]
        else:
            files = [Path(f) for f in files]

        if storage_url is None:
            storage_url = self._service_url + "/storage"

        if openghg_url is None:
            openghg_url = self._service_url + "/openghg"

        openghg = Service(service_url=openghg_url)
        creds = StorageCreds(user=user, service_url=storage_url)
        drive = Drive(creds=creds, name="openghg_drive")
        auth = Authorisation(resource="process", user=user)

        # Here we'll need special cases for different data types. As GCWERKS requires
        # both the data file and precision data and they need to be kept together
        # for use in processing.
        # We can maybe reconsider the way this is done if there ends up being a lot of test
        # cases and this gets a bit clunky

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
            response = self._service.call_function(function="process.process", args=args)

            if "Error" in response:
                if data_type in ("GCWERKS", "GC"):
                    filename = file[0].name
                else:
                    filename = file.name

                results[filename] = response["Error"]
            elif "results" in response:
                results.update(response["results"])

        return results
