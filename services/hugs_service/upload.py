from Acquire.Client import Drive, StorageCreds
from Acquire.ObjectStore import OSPar
from HUGS.ObjectStore import get_object_names, get_bucket

def upload(args):
    
    try:
        filename = args["filename"]
    except:
        filename = None

    drive_name = "hugs_files"
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(name=drive_name, creds=creds)

    filemeta = drive.upload(filename=filename, uploaded_name=filename)
    
    return {"result": filemeta.__dict__}

    # print(filemeta.__dict__)

    # try:
    #     prefix = args["prefix"]
    # except:
    #     prefix = None

    # bucket = get_bucket()
    # results = get_object_names(bucket=bucket, prefix=prefix)


