from ipywidgets import (GridspecLayout, GridBox, VBox, HBox, HTML, Layout,
                        Text, Button, Output, Checkbox, Label)
from random import randint
from ipywidgets import HBox, VBox
from pandas import read_json as pd_read_json
import numpy as np
import matplotlib.pyplot as plt
from numpy import random as np_random
import ipywidgets as widgets
from bqplot import DateScale, LinearScale, LogScale, Axis, Lines, Figure, Scatter
from bqplot import pyplot as plt
from Acquire.Client import User, Drive, Service, PAR, Authorisation, StorageCreds
from Acquire.ObjectStore import datetime_to_string
from HUGS.Client import Process, Search, Retrieve
from HUGS.Processing import search
import os
import sys
import secrets
import string
from datetime import datetime
sys.path.insert(0, "../../..")
sys.path.insert(0, "../../../../acquire")

__all__ = ["generate_password"]


def generate_password(length=20):
    selection = string.ascii_letters + string.digits
    return "".join(secrets.choice(selection) for _ in range(length))
