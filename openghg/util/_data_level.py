from typing import overload
import logging
from openghg.types import MetadataFormatError

__all__ = ["format_data_level"]

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


@overload
def format_data_level(
    data_level: str | int | float,
) -> str: ...


@overload
def format_data_level(
    data_level: None,
) -> None: ...


def format_data_level(data_level: str | int | float | None) -> str | None:
    """
    Check the input data_level to ensure this is in the expected format.

    This should follow the convention of:
        - "0": raw sensor output
        - "1": automated quality assurance (QA) performed
        - "2": final data set
        - "3": elaborated data products using the data

    Currently this will also allow 'decimal' inputs to indicate sub-levels (if required).
    This can be used as an alternative to the data_sublevel key.

    Args:
        data_level: Specified data level for data
    Returns:
        str: Formatted
    """
    from openghg.util import is_number

    msg_expected = "Expect: '0', '1', '2', '3' (or decimal to indicate sub-level)"

    if data_level is not None:
        if not is_number(data_level):
            msg = f"Unable to interpret data_level input: {data_level}. " + msg_expected
            logger.error(msg)
            raise MetadataFormatError(msg)

        data_level_check = float(data_level)
        if data_level_check < 0 or data_level_check >= 4:
            msg = f"Invalid input for data_level: {data_level}. " + msg_expected
            logger.error(msg)
            raise MetadataFormatError(msg)

        data_level = str(data_level)

    return data_level
