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
    if not isinstance(data, dict):
        try:   
            data = json.loads(data)
        except Exception:
            try:
                data = json.loads(data.getvalue())
            except Exception:
                tb = traceback.format_exc()
                return Response(ctx=ctx, response_data={"error": str(tb)})

    try:
        # The function we get passed should have a name such as
        # module.submodule 
        # where submodule and function are the same below
        module_function = data["function"]
        args = data["args"]

        split_fn = module_function.split(".")
        module_name = split_fn[0]
        function = split_fn[1] 

        # Here we import the module and function, which have the same name
        module = import_module(module_name)
        fn_to_call = getattr(module, function)

        # TODO - get each function to return the correct headers?
        # # response_data, headers = fn_to_call(arguments=arguments)
        response_data = fn_to_call(args=args)

        # # See https://stackoverflow.com/a/20509354
        # headers = {"Content-type": "application/json"}2021-06-16 16:25:46trace

        # Do we need a Response here or should we just use the data? 
        # Only the directly called Fn routing function needs to have a response really
        return response_data
        # return Response(ctx=ctx, response_data=response_data, headers=headers)
    except Exception as e:
        tb = traceback.format_exc()
        return {"error": str(e)}
    #     raise ModuleNotFoundError(f"{module_function} is not an OpenGHG service, {str(e)}")
    # except Exception:
    #     tb = traceback.format_exc()
    #     raise ValueError(f"Error calling {module_function} with error {str(tb)}")
