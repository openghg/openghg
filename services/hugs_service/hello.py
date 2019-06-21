

def run(args):

    try:
        name = args["name"]
    except:
        name = "World"

    if name == "no-one":
        raise PermissionError(
            "You cannot say hello to no-one!")

    greeting = "Hello %s" % name

    return {"greeting": greeting}
