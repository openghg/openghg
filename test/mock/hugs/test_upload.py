import pytest
import os

from Acquire.Client import Drive, StorageCreds

from HUGS.Client import Upload

def test_upload(authenticated_user):
    
    # args = {"authenticated_user":authenticated_user, "filename"=__file__}
    
    upload = Upload(service_url="hugs")
    
    filemeta = upload.upload(filename=__file__, authenticated_user=authenticated_user)

    print(filemeta)

    assert(False)
