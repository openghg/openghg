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

    print(args)


    # par_secret = args["par_secret"]
    # par_secret = hugs.decrypt_data(par_secret)
    
    job_data = args["requirements"]
    # # Pass the PAR through to allow use in the control script
    job_data["par"] = args["par"]
    # Pass the decrypted PAR secret here as we're on the server already
    # job_data["par_secret"] = par_secret 

    # # Upload any input files we need to be using to the cloud drive
    results = run_job(username="sshtest", hostname="127.0.0.1", job_data=job_data)

    return results
