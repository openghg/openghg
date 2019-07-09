
from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service


def hello(args):
    try:
        name = args["name"]
    except:
        name = "World"

    try:
        authorisation = Authorisation.from_data(args["authorisation"])
    except:
        authorisation = None

    try:
        par = PAR.from_data(args["file_par"])
    except:
        par = None

    try:
        par_secret = args["par_secret"]
    except:
        par_secret = None

    if authorisation:
        authorisation.verify("hello")
        name = "%s [authorised]" % authorisation.user_guid()

    if name == "no-one":
        raise PermissionError(
            "You cannot say hello to no-one!")

    greeting = "Hello %s" % name

    result = {"greeting": greeting}

    if par:
        # we need the hugs service with private access so that
        # we can decrypt the par_secret
        hugs = get_this_service(need_private_access=True)
        par_secret = hugs.decrypt_data(par_secret)

        # resolve the par to get the underlying file
        # (note that pars can be for files or directories)
        file = par.resolve(secret=par_secret)
        result["file"] = str(file)

        # now download the file to the /tmp directory - this will
        # time out if you don't specify the directory as the current
        # directory is read-only - the return value is the actual
        # name of the file that has been downloaded
        filename = file.download(dir="/tmp")
        result["filename"] = filename

        # here I am just reading the file to return it to you,
        # just to show that the file was correctly uploaded
        lines = open(filename).readlines()
        result["uploaded"] = lines

    return result

