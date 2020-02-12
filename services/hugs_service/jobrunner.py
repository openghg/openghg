from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Jobs import run_job

def job_runner(args):
    """ Service function that gets called by the Client job_runner

        Args:
            dict: Dictionary of variables used in setting up and running job
    """
    from HUGS.Jobs import SSHConnect

    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)
    # Verify that this process had authorisation to be called
    authorisation.verify("job_runner")

    # hugs = get_this_service(need_private_access=True)
    # par_secret = hugs.decrypt_data(par_secret)
    
    # This gives us access to the cloud drive through the PAR
    # drive = par.resolve(secret=par_secret)
    drive = par.resolve()

    job_data = args["requirements"]
    job_data["par"] = args["par"]

    # Upload any input files we need to be using to the cloud drive
    results = run_job(job_data=job_data, username="sshtest", hostname="127.0.0.1")

    return results
