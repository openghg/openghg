from ipywidgets import (GridspecLayout, GridBox, VBox, HBox, HTML, Layout, Text,
                        Button, Output, Checkbox, Label)
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
from datetime import datetime
sys.path.insert(0, "../../..")
sys.path.insert(0, "../../../../acquire")

__all__ = ["get_login"]

def get_login():
    login_text = HTML(value="<b>Please click the buton below to create a login link</b>")

    status_text = HTML(value=f"<font color='black'>Waiting for login</font>")
    login_button = Button(description="Login", button_style="success")
    login_link_box = Output()
    base_url = "https://hugs.acquire-aaai.com/t"
    user = User(username="gareth", identity_url=F"{base_url}/identity")

    def login(a):
        with login_link_box:
            response = user.request_login()

        if user.wait_for_login():
            status_text.value = f"<font color='green'>Login success</font>"
        else:
            status_text.value = f"<font color='red'>Login failure</font>"

    login_button.on_click(login)
    return user, VBox(children=[login_button, status_text, login_link_box])
    
    
