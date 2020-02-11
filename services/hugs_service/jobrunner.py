from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Jobs import run_job

def job_runner(args):
    from HUGS.Jobs import SSHConnect
    from .test_fn import test_function
    import yaml

    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)

    # Take the PAR and write some data to it
    par = PAR.from_data(args["par"])
    par_secret = args["par_secret"]

    # Verify that this process had authorisation to be called
    authorisation.verify("job_runner")

    hugs = get_this_service(need_private_access=True)

    par_secret = hugs.decrypt_data(par_secret)
    
    # This gives us access to the cloud drive through the PAR
    drive = par.resolve(secret=par_secret)

    job_data = args["requirements"]

    # Upload any input files we need to be using to the cloud drive
    results = run_job(job_data=job_data, username="sshtest", hostname="127.0.0.1")
    
    return {"results": 1}
