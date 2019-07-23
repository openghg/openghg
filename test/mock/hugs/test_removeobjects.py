import pytest
import os
import uuid

from HUGS.Client import Process
from HUGS.ObjectStore import get_bucket, get_object_names
from Acquire.Client import Service


def test_remove_objects(authenticated_user):
    bucket = get_bucket()

    # Get the Datasources in the bucket and remove them
    keys = get_object_names(bucket, prefix="datasource")

    top_three = keys[0:3]

    assert len(top_three) == 3

    args = {"keys": top_three}

    hugs = Service(service_url="hugs")
    _ = hugs.call_function(function="remove_objects", args=args)

    bucket = get_bucket()
    key_set = set(get_object_names(bucket, prefix="datasource"))
    three_set = set(top_three)
  
    # print(top_three)
    # print(key_set)

    assert not three_set.issubset(key_set)



    # creds = StorageCreds(user=authenticated_user, service_url="storage")
    # drive = Drive(creds=creds, name="test_drive")
    # filepath = os.path.join(os.path.dirname(__file__), "../../../test/data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat")
    # filemeta = drive.upload(filepath)

    # par = PAR(location=filemeta.location(), user=authenticated_user)

    # hugs = Service(service_url="hugs")
    # par_secret = hugs.encrypt_data(par.secret())

    # auth = Authorisation(resource="process", user=authenticated_user)

    # args = {"authorisation": auth.to_data(),
    #         "par": {"data": par.to_data()},
    #         "par_secret": {"data": par_secret},
    #         "data_type": "CRDS"}

    # response = hugs.call_function(function="process", args=args)
