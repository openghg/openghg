from Acquire.Client import Drive, StorageCreds
from Acquire.ObjectStore import OSPar
from HUGS.ObjectStore import get_object_names, get_bucket

def upload(args):
    
    try:
        filename = args["filename"]
    except:
        filename = None

    # Get authentication from data



    # Create a filehandle
    # Get authorisation
    # par_uid, secret and public key
    # Get the drive UID from the file handle 
    # Get a drive using DriveInfo

    # (filemeta, par) = drive.upload(filehandle=filehandle,
    #                             authorisation=authorisation,
    #                             encrypt_key=public_key,
    #                             par=par, identifiers=identifiers)





    # print(filemeta.__dict__)

    # try:
    #     prefix = args["prefix"]
    # except:
    #     prefix = None

    # bucket = get_bucket()
    # results = get_object_names(bucket=bucket, prefix=prefix)


