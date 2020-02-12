import logging
p_logger = logging.getLogger("parmiko")
p_logger.setLevel(logging.WARNING)


import datetime
import mockssh
import pytest
import os
import uuid

from HUGS.Client import Process
from HUGS.ObjectStore import get_local_bucket
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation, Location
from HUGS.Client import JobRunner

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

@pytest.fixture(autouse=True)
def run_before_tests():
    _ = get_local_bucket(empty=True)


@pytest.yield_fixture()
def server():
    users = {
        "wm19361": "~/.ssh/id_rsa_test",
    }
    with mockssh.Server(users) as s:
        yield s

def test_jobrunner(authenticated_user, tempdir):

    reqs = {}

    reqs["name"] = "test_name"
    reqs["run_command"] = "test_run_command"
    reqs["partition"] = "cpu_test"
    reqs["n_nodes"] = "test_n_nodes"
    reqs["n_tasks_per_node"] = "test_n_tasks_per_node"
    reqs["n_cpus_per_task"] = "test_n_cpus_per_task"
    reqs["memory_req"] = "128G"
    reqs["job_duration"] = "test_job_duration"

    # Get an authorisaton to pass to the service
    hugs = Service(service_url="hugs")
    # Credentials to create the cloud storage drive
    creds = StorageCreds(user=authenticated_user, service_url="storage")

    # Append a shortened UUID to the job name to ensure we don't multiple drives with the same name
    # short_uuid = create_uuid(short_uuid=True)
    job_name = "test_job"
    job_name = f"{job_name.lower()}_{123}"

    # Create a cloud drive for the input and output data to be written to
    drive = Drive(creds=creds, name=job_name)
    auth = Authorisation(resource="job_runner", user=authenticated_user)

    # Create a PAR with a long lifetime here and return a version to the user
    # and another to the server to allow writing of result data
    drive_guid = drive.metadata().guid()
    location = Location(drive_guid=drive_guid)
    
    par_lifetime = datetime.datetime.now() + datetime.timedelta(days=1)

    par = PAR(location=location, user=authenticated_user, expires_datetime=par_lifetime)
    par_secret = hugs.encrypt_data(par.secret())

    args = {}

    args["requirements"] = reqs
    args["par"] = par.to_data()
    args["par_secret"] = par_secret
    args["authorisation"] = auth.to_data()

    response = hugs.call_function(function="jobrunner", args=args)

    print(response)
    
    assert False
