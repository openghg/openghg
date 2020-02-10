__all__ = ["JobRunner"]

class JobRunner:
    def __init__(self, service_url):
        from Acquire.Client import Wallet

        wallet = Wallet()
        
        self._service = wallet.get_service(service_url=f"{service_url}/hugs")
        self._service_url = service_url

    def create_job(self, auth_user, job_name, requirements):
        """ Create a job

            Args:
                auth_user (Acquire.User): Authenticated Acquire user
                job_name (str): Name of job
                requirements (dict): Dictionary containing requested resources
                Example:
                    requirements = {"cores": 16, "memory": 128G, "duration": 12h}
                For a job running with 16 cores and requesting 128 GB of memory for 12 hours
                (if duration is required)
            Returns:
                dict: Dictionary containing information regarding job running on resource
                This will contain the PAR for access for data upload and download. 
        """
        from Acquire.Client import Drive, Service, PAR, Authorisation, StorageCreds, Location
        from Acquire.ObjectStore import create_uuid
        import datetime

        if self._service is None:
            raise PermissionError("Cannot use a null service")

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

        args = {}
        args["job_name"] = job_name
        args["requirements"] = requirements
        args["drive_par"] = par.to_data()

        response = self._service.call_function(function="job_runner", args=args)

        results = {}
        results["response"] = response
        results["par"] = par.to_data()

        return results

    def service(self):
        return self._service
