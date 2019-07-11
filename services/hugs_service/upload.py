from Acquire.Client import Drive, StorageCreds
from Acquire.ObjectStore import OSPar
from HUGS.ObjectStore import get_object_names, get_bucket

from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation

def upload(args):

  
    if "authorisation" in args:
        authorisation = args["authorisation"]
    else:
        raise KeyError("Authorisation required for upload")

    if "filename" in args:
        filename = args["filename"]
    else:
        raise KeyError("Filename required for upload")


    # Create the authorisation for the upload
    auth = Authorisation.from_data(authorisation)

    # Do the upload

    # Data will be uploaded to the drive
    # Can then pass a PAR to the HUGS function to access the data from a specific location
    # HUGS will then use the PAR to access the file and process it

    #


    # PAR here to access file
    #









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


