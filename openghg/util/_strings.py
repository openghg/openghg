from typing import Union, Optional, overload

__all__ = ["clean_string"]

@overload
def clean_string(to_clean: str) -> str: ...

@overload
def clean_string(to_clean: None) -> None: ...


def clean_string(to_clean: Optional[str]) -> Union[str, None]:
    """Returns a lowercase string with only alphanumeric
    characters.

    Args:
        to_clean: String to clean
    Returns:
        str or None: Clean string
    """
    import re

    if to_clean is None:
        return to_clean

    try:
        # Removes all whitespace
        cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
        # Removes non-alphanumeric characters
        cleanest = re.sub(r"\W+", "", cleaner)
    except TypeError:
        return to_clean

    return cleanest
