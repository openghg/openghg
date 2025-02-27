from collections.abc import Callable
from importlib import import_module
import inspect
from typing import Any, TypeVar


def get_parameters(func: Callable) -> list[str]:
    """Return list of parameters for a function."""
    return list(inspect.signature(func).parameters.keys())


CType = TypeVar("CType", bound=Callable)


class Registry:
    """Class to register functions so that they can be retrieved by name.

    Essentially, a "Registry" acts like a dictionary mapping strings to functions,
    so that the functions can be retrieve from this dictionary. This facilitates
    selecting functions by keywords in other functions or config files/dicts/etc.

    For example:

    >>> registry = Registry()
    >>>
    >>> @registry.register
    >>> def fun():
    >>>     pass
    >>>
    >>> assert "fun" in registry.functions
    >>> assert registry.functions["fun"] == fun

    A registry can format the function name:

    >>> registry = Registry(remove_suffix="parser")
    >>>
    >>> @registry.register
    >>> def icos_data_parser():
    >>>     pass
    >>>
    >>> assert "icos_data" in registry.functions
    >>> assert registry.functions["icos_data"] == icos_data_parser

    """

    def __init__(
        self, prefix: str = "", suffix: str = "", name_formatter: Callable[[str], str] | None = None
    ) -> None:
        """Setup Registry.

        Args:
            prefix: prefix to be removed from function to be registered.
            suffix: suffix to be removed from function to be registered.
            name_formatter: function to format the name of a registered function; the formatted name
                is used to retrieve the function from the registery.

                By default, the prefix and suffix are removed (including a trailing or leading "_", if
                the prefix or suffix is not empty).

        Returns:
            None
        """
        self._functions: dict[str, Callable] = {}

        if name_formatter is None:

            def _name_formatter(name: str) -> str:
                remove_prefix = prefix + "_" if prefix else ""
                remove_suffix = "_" + suffix if suffix else ""
                return name.removeprefix(remove_prefix).removesuffix(remove_suffix)

            name_formatter = _name_formatter

        self.name_formatter = name_formatter

    @property
    def functions(self) -> dict[str, Callable]:
        """Return dictionary of registered functions."""
        return self._functions

    def __getitem__(self, name: str) -> Callable:
        """Get function by name."""
        try:
            return self.functions[name]
        except KeyError as e:
            raise KeyError(f"No function registered with name {name}.") from e

    def __contains__(self, name: str) -> bool:
        """Check if function with given name is registered."""
        return name in self.functions

    def register(self, func: CType) -> CType:
        """Register a function.

        The function will be stored under its formatted name
        in `self.functions`.

        The function passed in is returned so that this method
        can be applied as a decorator.

        Args:
            func: function to register

        Returns:
            the same function that was passed in
        """
        self._functions[self.name_formatter(func.__name__)] = func
        return func

    def get_params(self, name: str) -> list[str]:
        """Return list of parameters accepted by registered function with given name."""
        try:
            return get_parameters(self.functions[name])
        except KeyError:
            raise ValueError(f"Function '{name}' not registered.")

    def select_params(self, name: str, params: dict, **kwargs: str) -> dict:
        """Select parameters from `params` that are accepted by function with given name.

        In addition, any parameter that starts with f"{name}__" will be selected, and the
        prefix f"{name}__" will be removed. This allows passing a common dictionary of arguments
        to several registered functions, while still allowing some arguments to apply to a single
        function.
        """
        fn_params = self.get_params(name)
        input_params = {**params, **kwargs}

        # first get common params
        selected_params = {k: v for k, v in input_params.items() if k in fn_params}

        # select params with special prefix
        prefix = f"{name}__"
        for k, v in input_params.items():
            if k.startswith(prefix) and k.removeprefix(prefix) in fn_params:
                selected_params[k.removeprefix(prefix)] = v

        return selected_params

    def describe(self) -> None:
        """Print names of registered functions and first line of their docstring."""
        for name, func in self.functions.items():
            try:
                first_line_of_docstring = func.__doc__.split("\n")[0]  # type: ignore
            except (AttributeError, IndexError):
                first_line_of_docstring = "No description."

            print(f"{name}: {first_line_of_docstring}")


class Locatable(type):
    """Metaclass that adds a location attribute to instances.

    The location records the module where the object was created.
    To infer the location before an object is created, we need to
    use a metaclass to change how the object is created.

    Note: the `__init__` method of any class using `Locatable` as a metaclass
    must have a keyword argument called `location`, or must accept **kwargs.
    """

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """If `location` is not passed during object creation, try to infer it.

        If the location (i.e. the module where the object is created) is found,
        add it to the kwargs passed when creating the object, and then create the
        object like normal.
        """
        location = kwargs.get("location")

        # try to infer location
        # https://stackoverflow.com/questions/22147050/how-to-know-where-an-object-was-instantiated-in-python
        if location is None:
            frame = inspect.currentframe()

            try:
                while frame:
                    if frame.f_code.co_name == "<module>":
                        kwargs["location"] = frame.f_locals["__name__"]
                        break
                    frame = frame.f_back
            finally:
                del frame

        # now create object in usual way
        return super().__call__(*args, **kwargs)


class AutoRegistry(Registry, metaclass=Locatable):
    """Registry that automatically registers functions with a specified suffix.

    Registration is attempted dynamically when functions are accessed, so that
    the definition of the registry can occur before the definition of functions
    that might be registered.

    The "location" of the registry must be the qualified name of a module in OpenGHG,
    for instance "openghg.standardise.surface".

    If the location is not passed directly, then it will be set to the location where the
    registry object is created. (Note: this is different from the place where AutoRegistry is
    defined, which is this file.)
    """

    def __init__(
        self,
        prefix: str = "",
        suffix: str = "",
        skip_private: bool = True,
        matcher: Callable[[str], bool] | None = None,
        name_formatter: Callable[[str], str] | None = None,
        location: str | None = None,
    ) -> None:
        """Setup AutoRegistry.

        Since functions are not (necessarily) registered by hand, the prefix
        and suffix arguments are used to check if a function should be registered.

        Alternatively, a "matcher" function can be provided to check if a function
        should be registered.

        Args:
            prefix: prefix of function to be registered.
            suffix: suffix of function to be registered.
            skip_private: if True, functions beginning with "_" will be ignored.
            matcher: this is applied to the name of a function to decide if it should
                be registered. If provided, this overrides `prefix`, `suffix`, and `skip_private`.
            name_formatter: function to format the name of a registered function; the formatted name
                is used to retrieve the function from the registery.

                By default, the prefix and suffix are removed (including a trailing or leading "_", if
                the prefix or suffix is not empty).
            location: fully qualified name of the module where the functions to be registered are defined.
                For instance, `openghg.standardise.surface` will register functions exported by the submodule.
                If None, then the location is set to the place where the registry is created.

        Returns:
            None
        """
        super().__init__(prefix, suffix, name_formatter)

        self.prefix = prefix
        self.suffix = suffix
        self.location = location

        if matcher is None:

            def _matcher(name: str) -> bool:
                if skip_private and name.startswith("_"):
                    return False
                return name.startswith(prefix) and name.endswith(suffix)

            matcher = _matcher
        self.matches = matcher

        self.functions_scanned = False

    @property
    def functions(self) -> dict[str, Callable]:
        """Return dictionary of registered functions.

        The first time this function is called, it scans the location of registry
        and compiles a list of functions
        """
        if self.location is None:
            raise RuntimeError(f"AutoRegistry {self} could not detect location where it was created.")

        if not self.functions_scanned:
            module = import_module(self.location)
            for name, value in module.__dict__.items():  # type: ignore
                if inspect.isfunction(value) and self.matches(name):
                    self.register(value)

            self.functions_scanned = True

        return super().functions
