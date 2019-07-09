
from Acquire.Client import Authorisation


def hello(args):

    try:
        name = args["name"]
    except:
        name = "World"

    try:
        authorisation = Authorisation.from_data(args["authorisation"])
    except:
        authorisation = None

    if authorisation:
        authorisation.verify("hello")
        name = "%s [authorised]" % authorisation.user_guid()

    if name == "no-one":
        raise PermissionError(
            "You cannot say hello to no-one!")

    greeting = "Hello %s" % name

    return {"greeting": greeting}
