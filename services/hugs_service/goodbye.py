def goodbye(args):
    try:
        name = args["name"]
    except:
        name = "John"

    if name == "no-one":
        raise PermissionError("You cannot say goodbye to no-one!")

    greeting = "Goodbye %s" % name

    return {"greeting": greeting}
