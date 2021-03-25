__all__ = ["clean_string"]


def clean_string(to_clean):
    """ Returns a string that is purely alphanumeric with no whitespace
        or punctuation

        Args:
            to_clean: String to clean
        Returns:
            str: Clean string
    """
    import re
    cleaner = re.sub(r"\s+", "", to_clean, flags=re.UNICODE).lower()
    cleanest = re.sub(r"\W+", "", cleaner)

    return cleanest
