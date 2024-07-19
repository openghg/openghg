from typing import Optional
import logging
import re
from openghg.types import MetadataFormatError

__all__ = ["format_data_level"]

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


def format_data_level(data_level: Optional[str]) -> Optional[str]:
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

    msg_expected = "Expect: '0', '1', '2', '3' (or decimal to indicate sub-level)"

    if data_level is not None:
        data_level = str(data_level)
        try:
            number = float(data_level)
        except (ValueError, TypeError):
            msg = f"Unable to interpret data_level input: {data_level}. " + msg_expected 
            logger.error(msg)
            raise MetadataFormatError(msg)
        else:
            if number >= 4:
                msg = "data_level input contains a number > 3: {data_level}. " + msg_expected
                logger.error(msg)
                raise MetadataFormatError(msg)

    return data_level
