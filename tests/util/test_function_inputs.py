import pytest
from openghg.util import split_function_inputs


def fn(site, inlet, species, return_values=True):
    """
    Simple function used for testing below.
    Args:
        site, inlet, species: 3 positional arguments
        return_values: determines whether the values within positional
            arguments are returned.
    """
    if return_values:
        return site, inlet, species


def test_split_function_inputs():
    """
    Check correct parameters can be selected for function and function
    can be called correctly
    """

    parameters = {"site": "tac",
                  "inlet": "10m",
                  "species": "ch4",
                  "return_values": False}
    
    fn_inputs, remaining_inputs = split_function_inputs(parameters, fn)
    
    # Expect all parameters to be found and included in dictionary
    assert fn_inputs == parameters
    assert remaining_inputs == {}

    output = fn(**fn_inputs)

    # Expect return_values=False to be passed - no output returned
    assert output is None


def test_split_function_inputs_default():
    """
    Check correct parameters can be selected for function when
    not all input keywords are specified. 
    Check function can be called correctly and use defaults.
    """

    parameters = {"site": "tac",
                  "inlet": "10m",
                  "species": "ch4"}
    
    fn_inputs, remaining_inputs = split_function_inputs(parameters, fn)

    # Expect selected parameters to be found and included in dictionary
    assert fn_inputs == parameters
    assert remaining_inputs == {}

    output = fn(**fn_inputs)

    # Expect default return_values=True to be used - output returned
    assert output == ("tac", "10m", "ch4")


def test_split_function_inputs_extra():
    """
    Check correct parameters can be selected for function when
    additional keywords are specified.
    Check function can be called correctly.
    """

    parameters = {"site": "tac",
                  "inlet": "10m",
                  "species": "ch4",
                  "additional_key": "not_needed"}
    
    fn_inputs, remaining_inputs = split_function_inputs(parameters, fn)

    # Remove key which does not match to the function call for checking
    expected_parameters = parameters.copy()
    expected_parameters.pop("additional_key")

    expected_remaining = {"additional_key": "not_needed"}

    # Expect selected parameters to be found and included in dictionary
    assert fn_inputs == expected_parameters
    assert remaining_inputs == expected_remaining

    output = fn(**fn_inputs)

    assert output == ("tac", "10m", "ch4")


def test_split_function_inputs_not_all():
    """
    Check correct parameters can be selected for function even when
    required keys are missing.
    Check this raises expected TypeError when function is called.

    Note: in split_function_inputs could be updated to catch this but may
    be easier to let function itself raise relevant error message.
    """

    parameters = {"species": "ch4"}
    
    fn_inputs, remaining_inputs = split_function_inputs(parameters, fn)

    assert fn_inputs == parameters
    assert remaining_inputs == {}

    with pytest.raises(TypeError) as excinfo:
        fn(**fn_inputs)
        assert "missing 2 required positional arguments: 'site' and 'inlet'" in excinfo
