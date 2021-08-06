from io import BytesIO
from fdk.response import Response
from fdk.context import InvokeContext
from importlib import import_module
from typing import Dict

import traceback


def route(function_name: str, data: Dict) -> Dict:
    """Route the call to a specific function

    Args:
        ctx: Invoke context. This is passed by Fn to the function
        data: Data passed to the function by the user
    Returns:
        Response: Fn FDK response object containing function call data
        and data returned from function call
    """
    try:
        # The function we get passed should have a name such as
        # module.submodule
        # where submodule and function are the same below
        module_function = function_name

        try:
            split_fn = module_function.split(".")
            module_name = split_fn[0]
            function = split_fn[1]
        except IndexError:
            raise ValueError(
                "Incorrect function format, please pass function name of type <service_file>.<service_fn>"
            )

        # TODO - This needs to be fixed so the import of services works in the Docker image

        # Here we import the module and function, which have the same name
        try:
            module = import_module(module_name)
        except ModuleNotFoundError:
            module = import_module(f"openghg_services.{module_name}")

        fn_to_call = getattr(module, function)

        response_data: Dict = fn_to_call(args=data)

        return response_data
    except Exception:
        return {"Error": traceback.format_exc()}


async def handle_invocation(ctx: InvokeContext, data: BytesIO) -> Response:
    """The endpoint for the function. This handles the POST request and passes it through
    to the handler

    Args:
        ctx: Invoke context. This is passed by Fn to the function
        data: Data passed to the function by the user
    Returns:
        Response: Fn FDK response object containing function call data
        and data returned from function call
    """
    import traceback

    try:
        data = data.getvalue()
    except Exception:
        return {"Error": traceback.format_exc()}
        # return Response(ctx=ctx, response_data=error_str)

    returned_data = route(data=data)
    # headers = {"Content-Type": "application/octet-stream"}

    # return Response(ctx=ctx, response_data=returned_data, headers=headers)
    return returned_data
