from io import BytesIO
import json
from fdk.context import InvokeContext
from fdk.response import Response
from importlib import import_module
from typing import Dict, Union
import traceback


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
        data = json.loads(data)
    except Exception:
        try:
            data = json.loads(data.getvalue())
        except Exception:
            tb = traceback.format_exc()
            return Response(ctx=ctx, response_data={"error": str(tb)})

    try:
        function_name = data["function"]
        args = data["args"]

        base_module_name = "openghg_services"
        module_name = ".".join((base_module_name, function_name))

        # Here we import the module and function from the openghg_services module
        module = import_module(module_name)
        fn_to_call = getattr(module, function_name)

        # TODO - get each function to return the correct headers?
        # # response_data, headers = fn_to_call(arguments=arguments)
        response_data = fn_to_call(args=args)

        # # See https://stackoverflow.com/a/20509354
        headers = {"Content-type": "application/json"}

        return Response(ctx=ctx, response_data=response_data, headers=headers)
    except Exception:
        tb = traceback.format_exc()
        return Response(ctx=ctx, response_data={"error": str(tb)})
