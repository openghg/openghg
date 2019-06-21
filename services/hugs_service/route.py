

def hugs_functions(function, args):
    """These are all of the functions for the hug service"""
    if function == "hello":
        from hugs_service.hello import run as _hello
        return _hello(args)
    else:
        from admin.handler import MissingFunctionError
        raise MissingFunctionError()


if __name__ == "__main__":
    import fdk
    from admin.handler import create_async_handler
    fdk.handle(create_async_handler(hugs_functions))
