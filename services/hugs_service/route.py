

def hugs_functions(function, args):
    """These are all of the functions for the hug service"""
    if function == "hello":
        from hugs_service.hello import hello as _hello
        return _hello(args)
    if function == "goodbye":
        from hugs_service.goodbye import goodbye as _goodbye
        return _goodbye(args)
    if function == "listobjects":
        from hugs_service.listobjects import listobjects as _listobjects
        return _listobjects(args)
    if function == "search":
        from hugs_service.search import search as _search
        return _search(args)
    if function == "process":
        from hugs_service.process import process as _process
        return _process(args)
    if function == "retrieve":
        from hugs_service.retrieve import retrieve as _retrieve
        return _retrieve(args)
    if function == "remove_objects":
        from hugs_service.removeobjects import remove_objects as _remove_objects
        return _remove_objects(args)

    else:
        from admin.handler import MissingFunctionError
        raise MissingFunctionError()


if __name__ == "__main__":
    import fdk
    from admin.handler import create_async_handler
    fdk.handle(create_async_handler(hugs_functions))
