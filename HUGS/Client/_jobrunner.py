__all__ = ["JobRunner"]

class JobRunner:
    """ An interface to the the jobrunner service on the HUGS platform.

        This class is used to run jobs on both local and cloud based HPC clusters

        Args:
            service_url (str): URL of service
    """
    def __init__(self, service_url):
        from Acquire.Client import Wallet

        wallet = Wallet()
        
        self._service = wallet.get_service(service_url=f"{service_url}/hugs")
        self._service_url = service_url

    def create_job(self, auth_user, requirements, key_password, data_files, hugs_url=None, storage_url=None):
        """ Create a job

            Args:
                auth_user (Acquire.User): Authenticated Acquire user

                The following keys are required:
                    "hostname", "username", "name", "run_command", "partition", "n_nodes", "n_tasks_per_node", 
                    "n_cpus_per_task", "memory_req", "job_duration"
                where partition must be one of:
                    "cpu_test", "dcv", "gpu", "gpu_veryshort", "hmem", "serial", "test", "veryshort"

                Example:
                    requirements = {"hostname": hostname, "username": username, "name": "test_job, "n_nodes": 2, 
                                    "n_tasks_per_node": 2, "n_cpus_per_task": 2, "memory": "128G", ...}

                requirements (dict): Dictionary containing job details and requested resources
                key_password (str): Password for private key used to access the HPC
                data_files (dict): Data file(s) to be uploaded to the cloud drive to 
                run the simulation. Simulation code files should be given in the "app" key and data
                files in the "data" key

                TODO - having to pass in a password and get it through to Paramiko seems
                long winded, is there a better way to do this?

                hugs_url (str): URL of HUGS service
                storage_url (str): URL of storage service
            Returns:
                dict: Dictionary containing information regarding job running on resource
                This will contain the PAR for access for data upload and download. 
        """
        from Acquire.Client import Drive, Service, PAR, Authorisation, StorageCreds, Location, ACLRule
        from Acquire.ObjectStore import create_uuid
        import datetime
        import os

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        if storage_url is None:
            storage_url = self._service_url + "/storage"

        if hugs_url is None:
            hugs_url = self._service_url + "/hugs"

        if not isinstance(data_files["app"], list):
            data_files["app"] = [data_files["app"]]

        if not isinstance(data_files["data"], list):
            data_files["data"] = [data_files["data"]]

        # Get an authorisaton to pass to the service
        hugs = Service(service_url=hugs_url)
        # Credentials to create the cloud storage drive
        creds = StorageCreds(user=auth_user, service_url=storage_url)

        # Append a shortened UUID to the job name to ensure we don't multiple drives with the same name
        short_uuid = create_uuid(short_uid=True)

        job_name = requirements["name"]
        job_name = f"{job_name.lower()}_{short_uuid}"
        
        # Create a cloud drive for the input and output data to be written to
        drive = Drive(creds=creds, name=job_name)

        # Check the size of the files and if we want to use the chunk uploader
        # Now we want to upload the files to the cloud drive we've created for this job
        chunk_limit = 50*1024*1024

        # Store the metadata for the uploaded files
        uploaded_files = {}
        # These probably won't be very big so don't check their size
        for f in data_files["app"]:
            file_meta = drive.upload(f, dir="app")
            uploaded_files["app"][f] = file_meta

        for f in data_files["data"]:
            if filesize < chunk_limit:
                    file_meta = drive.upload(f, dir="data")
                else:
                    file_meta = drive.chunk_upload(f, dir="data")

                uploaded_files["data"][f] = file_meta

        auth = Authorisation(resource="job_runner", user=auth_user)
        # Create a PAR with a long lifetime here and return a version to the user
        # and another to the server to allow writing of result data
        drive_guid = drive.metadata().guid()
        location = Location(drive_guid=drive_guid)

        # Read the duration from the requirements dictionary

        # TODO - add in some reading of the duration
        # try:
        #     duration = requirements["duration"]
        #     par_expiry = datetime.datetime
    
        par_lifetime = datetime.datetime.now() + datetime.timedelta(days=1)

        # Create an ACL rule for this PAR so we can read and write to it
        aclrule = ACLRule.owner()
        par = PAR(location=location, user=auth_user, aclrule=aclrule, expires_datetime=par_lifetime)
       
        par_secret = par.secret()
        encryped_par_secret = hugs.encrypt_data(par_secret)

        # Encrypt the password we use to decrypt the private key used to access the HPC cluster
        # TODO - is this a sensible way of doing this?
        encrypted_password = hugs.encrypt_data(key_password)

        par_data = par.to_data()
    
        args = {}
        args["authorisation"] = auth.to_data()
        args["par"] = par_data
        args["par_secret"] = encryped_par_secret
        args["requirements"] = requirements
        args["key_password"] = encrypted_password

        function_response = self._service.call_function(function="job_runner", args=args)

        response = {}
        response["function_response"] = function_response
        response["par"] = par_data
        response["par_secret"] = par_secret
        response["uploaded_files"] = uploaded_files

        return response

    def service(self):
        return self._service
