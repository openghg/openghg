__all__ = ["clean_string"]


def clean_string(to_clean: str) -> str:
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

    # Removes all whitespace
    cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
    # Removes non-alphanumeric characters
    cleanest = re.sub(r"\W+", "", cleaner)

    return cleanest
