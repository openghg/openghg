from io import BytesIO
import json
from fdk.context import InvokeContext
from fdk.response import Response
import logging
from importlib import import_module
from typing import Dict, Union


def route(ctx: InvokeContext, data: Union[Dict, BytesIO]) -> Response:
    """ Route the call to a specific function

        Args:
            ctx: Invoke context. This is passed by Fn to the function
            data: Data passed to the function by the user
        Returns:
            Response: Fn FDK response object containing function call data
            and data returned from function call
    """
    try:   
        data = json.loads(data.getvalue())
    except AttributeError:
        try:
            data = json.loads(data)
        except Exception as e:
            logging.getLogger().info(f"Error parsing JSON argument: {e}")
            raise ValueError(f"Cannot read function data. Error: {e}")

    function_name = data["function"]
    args = data["args"]

    # Here we import the module and function from the hugs_service module
    # Need to add openghg_services. to the start
    base_module_name = "openghg_services"
    module_name = ".".join((base_module_name, function_name))

    # Here we import the module and function from the openghg_services module
    # Need to add openghg_services. to the start
    try:
        module = import_module(module_name)
        fn_to_call = getattr(module, function_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise AttributeError(f"Error calling function {function_name}. Error: {e}")

    # Get the data and headers here?
    # response_data, headers = fn_to_call(arguments=arguments)
    response_data = fn_to_call(args=args)
    # See https://stackoverflow.com/a/20509354
    headers = {"Content-type": "application/json"}

    return Response(ctx=ctx, response_data=response_data, headers=headers)
