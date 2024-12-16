from typing import Any
from collections.abc import Callable
import logging
import inspect

logger = logging.getLogger("openghg.util.function_inputs")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


__all__ = ["split_function_inputs"]


def split_function_inputs(
    parameters: dict[str, Any], fn: Callable, warning: bool = False
) -> tuple[dict, dict]:
    """
    Check set of parameters against a function and split the keys the function accepts and doesn't accept.

    Args:
        parameters: Dictionary of potential paraeters to pass to a function
        fn: Function to check parameters against
        warning: Whether to raise a warning if a parameter from parameters does not match to the function
    Returns:
        dict, dict: Dictionaries for parameters accepted and not accepted by the function
    """
    # Find parameters that fn accepts
    signature = inspect.signature(fn)
    fn_accepted_parameters = [param.name for param in signature.parameters.values()]

    fn_parameters = {}
    remaining_parameters = {}
    for param, param_value in parameters.items():
        if param in fn_accepted_parameters:
            fn_parameters[param] = param_value
        else:
            remaining_parameters[param] = param_value
            if warning:
                logger.warning(f"Input: '{param}' (value: {param_value}) is not being passed to: {fn}")

    return fn_parameters, remaining_parameters
