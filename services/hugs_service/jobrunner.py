from Acquire.Client import Authorisation
from Acquire.Service import get_this_service

from HUGS.Jobs import run_job


def job_runner(args):
    """ Service function that gets called by the Client job_runner

        Args:
            dict: Dictionary of variables used in setting up and running job
        Returns:
            dict: Dictionary of data detailing job run status such as stdout, stderr output
    """
    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)
    # Verify that this process had authorisation to be called
    authorisation.verify("job_runner")

    hugs = get_this_service(need_private_access=True)

    job_data = args["requirements"]
    # # Pass the PAR through to allow use in the control script
    job_data["par"] = args["par"]
    # Pass the decrypted PAR secret here as we're on the server already
    job_data["par_secret"] = hugs.decrypt_data(args["par_secret"])

    hostname = job_data["hostname"]
    username = job_data["username"]

    # Have we used this server before?
    try:
        known_host = job_data["known_host"]
    except KeyError:
        known_host = False

    # Decrypt the password we use to access the private key
    password = hugs.decrypt_data(args["key_password"])

    results = run_job(
        username=username,
        hostname=hostname,
        password=password,
        job_data=job_data,
        known_host=known_host,
    )

    return results
