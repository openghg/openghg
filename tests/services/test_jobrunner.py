# import logging
# p_logger = logging.getLogger("parmiko")
# p_logger.setLevel(logging.WARNING)


# import datetime
# import mockssh
# import pytest
# import os
# import uuid

# from openghg.client import Process
# from openghg.objectstore import get_bucket
# from Acquire.Client import ACLRule, User, Drive, Service, StorageCreds, PAR, Authorisation, Location
# from Acquire.ObjectStore import ObjectStore
# from openghg.client import JobRunner
# from openghg.jobs import JobDrive

# @pytest.fixture(scope="session")
# def tempdir(tmpdir_factory):
#     d = tmpdir_factory.mktemp("")
#     return str(d)

# @pytest.fixture(autouse=True)
# def run_before_tests():
#     _ = clear_test_store()


# @pytest.yield_fixture()
# def server():
#     users = {
#         "wm19361": "~/.ssh/id_rsa_test",
#     }
#     with mockssh.Server(users) as s:
#         yield s

# def test_jobrunner(authenticated_user, tempdir):
#     reqs = {}

#     reqs["name"] = "test_name"
#     reqs["run_command"] = "test_run_command"
#     reqs["partition"] = "cpu_test"
#     reqs["n_nodes"] = "test_n_nodes"
#     reqs["n_tasks_per_node"] = "test_n_tasks_per_node"
#     reqs["n_cpus_per_task"] = "test_n_cpus_per_task"
#     reqs["memory_req"] = "128G"
#     reqs["job_duration"] = "test_job_duration"

#     # Get an authorisaton to pass to the service
#     hugs = Service(service_url="hugs")
#     # Credentials to create the cloud storage drive
#     creds = StorageCreds(user=authenticated_user, service_url="storage")

#     # Append a shortened UUID to the job name to ensure we don't multiple drives with the same name
#     # short_uuid = create_uuid(short_uuid=True)
#     job_name = "test_job"
#     job_name = f"{job_name.lower()}_{123}"

#     # Create a cloud drive for the input and output data to be written to
#     drive = Drive(creds=creds, name=job_name)
#     auth = Authorisation(resource="job_runner", user=authenticated_user)

#     # Create a PAR with a long lifetime here and return a version to the user
#     # and another to the server to allow writing of result data
#     drive_guid = drive.metadata().guid()
#     location = Location(drive_guid=drive_guid)

#     par_lifetime = datetime.datetime.now() + datetime.timedelta(days=1)
#     par = PAR(location=location, user=authenticated_user,
#   expires_datetime=par_lifetime, aclrule=ACLRule.writer())
#     par_secret = openghg.encrypt_data(par.secret())

#     password = os.environ["RUNNER_PWD"]
#     encrypted_password = openghg.encrypt_data(password)

#     args = {}
#     args["authorisation"] = auth.to_data()
#     args["par"] = par.to_data()
#     args["par_secret"] = par_secret
#     args["requirements"] = reqs
#     args["key_password"] = encrypted_password

#     response = openghg.call_function(function="job_runner", args=args)

#     print(response)

#     assert False
