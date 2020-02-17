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

    def create_job(self, auth_user, requirements, hugs_url=None, storage_url=None):
        """ Create a job

            Args:
                auth_user (Acquire.User): Authenticated Acquire user
                requirements (dict): Dictionary containing job details and requested resources

                The following keys are required:
                    "name", "run_command", "partition", "n_nodes", "n_tasks_per_node", 
                    "n_cpus_per_task", "memory_req", "job_duration"
                where partition must be one of:
                    "cpu_test", "dcv", "gpu", "gpu_veryshort", "hmem", "serial", "test", "veryshort"

                Example:
                    requirements = {"name": "test_job, "n_nodes": 2, "n_tasks_per_node": 2, 
                                    "n_cpus_per_task": 2, "memory": "128G", ...}

                hugs_url (str): URL of HUGS service
                storage_url (str): URL of storage service
            Returns:
                dict: Dictionary containing information regarding job running on resource
                This will contain the PAR for access for data upload and download. 
        """
        from Acquire.Client import Drive, Service, PAR, Authorisation, StorageCreds, Location
        from Acquire.ObjectStore import create_uuid
        import datetime

        if self._service is None:
            raise PermissionError("Cannot use a null service")

        if storage_url is None:
            storage_url = self._service_url + "/storage"

        if hugs_url is None:
            hugs_url = self._service_url + "/hugs"

        # Get an authorisaton to pass to the service
        hugs = Service(service_url=hugs_url)
        # Credentials to create the cloud storage drive
        creds = StorageCreds(user=auth_user, service_url=storage_url)

        # Append a shortened UUID to the job name to ensure we don't multiple drives with the same name
        short_uuid = create_uuid(short_uuid=True)
        job_name = f"{job_name.lower()}_{short_uuid}"
        
        # Create a cloud drive for the input and output data to be written to
        drive = Drive(creds=creds, name=job_name)
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

        par = PAR(location=location, user=auth_user, expires_datetime=par_lifetime)
        # par_secret = hugs.encrypt_data(par.secret())

        args = {}
        
        args["requirements"] = requirements
        args["par"] = par.to_data()
        # args["par_secret"] = par_secret

        response = self._service.call_function(function="job_runner", args=args)

        print(response)

        return False

        # results = {}
        # results["response"] = response
        # results["par"] = par.to_data()

        # return results

    def service(self):
        return self._service
