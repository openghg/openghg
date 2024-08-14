from typing import Callable, Any
import logging
import inspect

logger = logging.getLogger("openghg.util.function_inputs")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


__all__ = ["match_function_inputs"]


def match_function_inputs(parameters: dict[str, Any], fn: Callable, warning: bool = False) -> dict[str, Any]:
    """
    Check set of paramaters against a function and select the keys the function accepts.

    Args:
        parameters: Dictionary of potential parameters to pass to a function
        fn: Function to check parameters against
    Returns:
        dict: Dictionary of parameters accepted by the function
    """
    # Find parameters that fn accepts
    signature = inspect.signature(fn)
    fn_accepted_parameters = [param.name for param in signature.parameters.values()]

    fn_parameters = {}
    for param, param_value in parameters.items():
        if param in fn_accepted_parameters:
            fn_parameters[param] = param_value
        elif warning:
            logger.warning(f"Input: '{param}' (value: {param_value}) is not being passed to: {fn}")

    return fn_parameters
