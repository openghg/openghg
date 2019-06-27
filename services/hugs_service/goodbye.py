def goodbye(args):
    try:
        name = args["name"]
    except:
        name = "World"

    if name == "no-one":
        raise PermissionError(
            "You cannot say hello to no-one!")

    greeting = "Goodbye %s" % name

    return {"greeting": greeting}