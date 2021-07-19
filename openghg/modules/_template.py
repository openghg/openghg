from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

# flake8: noqa

__all__ = ["TEMPLATE"]

# To use this template replace:
# - TEMPLATE with new data name in all upper case e.g. CRDS
# - template with new data name in all lower case e.g. crds
# - CHANGEME with a new fixed uuid (at the moment)


class TEMPLATE(BaseModule):
    """Class for processing TEMPLATE data"""

    _root = "TEMPLATE"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _uuid = "CHANGEME"

    def __init__(self):
        raise NotImplementedError

    
