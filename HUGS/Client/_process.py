__all__ = ["Process"]


class Process:
    """ Process a datafile at a given PAR

    """
    def __init__(self, service_url=None):
        """ Process a datafile using the passed user account

            service_url = "https://hugs.acquire-aaai.com/t"        

            Args:
                service_url: URL of service 
        """
        if service_url:
            from Acquire.Client import Wallet as _Wallet
            wallet = _Wallet()
            self._service = wallet.get_service(service_url="%s/hugs" % service_url)
            self._service_url = service_url
        else:
            self._service

    def process_folder(self, user, folder_path, data_type, source_name=None, overwrite=False, extension="dat", 
                        hugs_url=None, storage_url=None, instrument=None):
        """ Process the passed directory of data files

            Args:
                user (User): Authenticated Acquire User
                folder (str, list): Path of files to be processed
                data_type (str): Type of data to be processed (CRDS, GC etc)
                hugs_url (str): URL of HUGS service. Currently used for testing
                datasource (str): Datasource name or UUID
                This may be removed in the future.
                storage_url (str): URL of storage service. Currently used for testing
                This may be removed in the future.
        """
        from pathlib import Path
        import os

        if data_type == "GC":
            filepaths = []
            # Find all files in 
            for f in Path(folder_path).glob(f"**/*.C"):
                if "precisions" in f.name:
                    # Remove precisions section and ensure file exists
                    data_filename = str(f).replace(".precisions", "")
                    if Path(data_filename).exists():
                        filepaths.append((Path(data_filename), f))
        elif data_type == "CRDS":
            filepaths = [f for f in Path(folder_path).glob(f'**/*.{extension}')]
        else:
            raise NotImplementedError("Currently only implemented for CRDS and GC data types")

        return self.process_files(user=user, files=filepaths, data_type=data_type, source_name=source_name, overwrite=overwrite,
                                    instrument=instrument)

    # Find a better way to get this storage url in here, currently used for testing
    def process_files(self, user, files, data_type, source_name=None, overwrite=False, hugs_url=None, 
                        storage_url=None, datasource=None, site=None, instrument=None):
        """ Process the passed file(s) 

            Args:
                user (User): Authenticated Acquire User
                files (str, list): Path of files to be processed
                data_type (str): Type of data to be processed (CRDS, GC etc)
                hugs_url (str): URL of HUGS service. Currently used for testing
                datasource (str): Datasource name or UUID

                TODO - update this

                This may be removed in the future.
                storage_url (str): URL of storage service. Currently used for testing
                This may be removed in the future.
            Returns:
                dict: UUID of processed files keyed by filename
        """
        from Acquire.Client import Drive, Service, PAR, Authorisation, StorageCreds
        import os

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        if not isinstance(files, list):
            files = [files]

        if data_type.upper() == "GC":
            if not all(isinstance(item, tuple) for item in files):
                return TypeError("If data type is GC, a list of tuples for data and precision filenames must be passed")

        if storage_url is None:
            storage_url = self._service_url + "/storage"

        if hugs_url is None:
            hugs_url = self._service_url + "/hugs"

        # # Take the filename without the file extension
        # source_name = [os.path.splitext((filepath.name).split("/")[-1])[0] for filepath in files]
        
        hugs = Service(service_url=hugs_url)
        creds = StorageCreds(user=user, service_url=storage_url)
        drive = Drive(creds=creds, name="test_drive")
        auth = Authorisation(resource="process", user=user)

        # Here we'll need special cases for different data types. As GC requires
        # both the data file and precision data and they need to be kept together
        # for use in processing.
        # We can maybe reconsider the way this is done if there ends up being a lot of test
        # cases and this gets a bit clunky
        datasource_uuids = {}
        for file in files:
            if data_type.upper() == "GC":
                # This is only used as a key when returning the Datasource UUIDs
                filename = file[0].name
                # This may be removed in the future as is currently only for testing
                
                if not source_name:
                    source_name = os.path.splitext(filename)[0]

                # TODO - update this so we get proper reading of parameters
                # dictionary
                if not site:
                    site = source_name.split(".")[0]
                
                filemeta = drive.upload(file[0])
                par = PAR(location=filemeta.location(), user=user)
                par_secret = hugs.encrypt_data(par.secret())

                prec_meta = drive.upload(file[1])
                prec_par = PAR(location=prec_meta.location(), user=user)
                prec_par_secret = hugs.encrypt_data(prec_par.secret())

                args = { "authorisation": auth.to_data(),
                        "par": {"data": par.to_data(), "precision": prec_par.to_data()},
                        "par_secret": {"data": par_secret, "precision": prec_par_secret},
                        "data_type": data_type, "datasource": datasource,
                        "source_name":source_name, "overwrite": overwrite,
                        "site":site, "instrument":instrument }
            else:
                filename = file[0].name
                
                filemeta = drive.upload(file)
                par = PAR(location=filemeta.location(), user=user)
                par_secret = hugs.encrypt_data(par.secret())

                args = { "authorisation": auth.to_data(),
                        "par": {"data": par.to_data()},
                        "par_secret": {"data": par_secret},
                        "data_type": data_type, "datasource": datasource,
                        "source_name":source_name, "overwrite":overwrite}

            # If we try to upload many files we don't want it to fail if a single
            # file contains overlapping data
            try:
                response = self._service.call_function(function="process", args=args)
                datasource_uuids[filename] = response["results"]
            except ValueError as err:
                datasource_uuids[filename] = err

        return datasource_uuids

    # def process_file(self, auth, par, par_secret, data_type):
    #     """ Pass a PAR for the file to be processed to the processing function

    #         Args:
    #             par : JSON serialised PAR object

    #     """
    #     if self._service is None:
    #         raise PermissionError("Cannot use a null service")

    #     args = {"authorisation": auth.to_data(),
    #             "par": {"data": par.to_data()},
    #             "par_secret": {"data": par_secret},
    #             "data_type": "CRDS"}

    #     response = self._service.call_function(function="process", args=args)

    #     return response["results"]
