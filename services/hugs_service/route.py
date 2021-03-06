def hugs_functions(function, args):
    """ These are all of the functions for the hug service

        Args:
            function (str): Name of function to call
            args (dict): Dictionary of arguments
        Returns:
            dict: Dictionary of results
    """
    if function == "listobjects":
        from hugs_service.listobjects import listobjects

        return listobjects(args)
    if function == "search":
        from hugs_service.search import search

        return search(args)
    if function == "process":
        from hugs_service.process import process

        return process(args)
    if function == "retrieve":
        from hugs_service.retrieve import retrieve

        return retrieve(args)
    if function == "remove_objects":
        from hugs_service.removeobjects import remove_objects

        return remove_objects(args)
    if function == "clear_datasources":
        from hugs_service.cleardatasources import clear_datasources

        return clear_datasources(args)
    if function == "status":
        from hugs_service.status import status

        return status()
    if function == "job_runner":
        from hugs_service.jobrunner import job_runner

        return job_runner(args)
    if function == "rank_sources":
        from hugs_service.rank_sources import rank_sources

        return rank_sources(args)
    if function == "get_sources":
        from hugs_service.get_sources import get_sources

        return get_sources(args)
    else:
        from admin.handler import MissingFunctionError

        raise MissingFunctionError()


if __name__ == "__main__":
    import fdk
    from admin.handler import create_async_handler

    fdk.handle(create_async_handler(hugs_functions))
