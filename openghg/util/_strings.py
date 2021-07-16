from typing import Union

__all__ = ["clean_string"]


def clean_string(to_clean: str) -> Union[str, None]:
    """Returns a lowercase string with only alphanumeric
    characters.

    Args:
        to_clean: String to clean
    Returns:
        str: Clean string
    """
    import re

    if to_clean is None:
        return

    try:
        # Removes all whitespace
        cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
        # Removes non-alphanumeric characters
        cleanest = re.sub(r"\W+", "", cleaner)
    except TypeError:
        return to_clean

    return cleanest
