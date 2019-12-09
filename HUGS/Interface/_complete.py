from ipywidgets import (GridspecLayout, GridBox, VBox, HBox, HTML, Layout, Text,
                        Button, Output, Checkbox, Label)
from random import randint
import numpy as np
from numpy import random as np_random
from bqplot import DateScale, LinearScale, Axis, Lines, Figure
from bqplot import pyplot as plt
from Acquire.Client import User, Drive, Service, PAR, Authorisation, StorageCreds
from HUGS.Processing import search
import ipywidgets as widgets
import os
import sys
sys.path.insert(0, "../../..")
sys.path.insert(0, "../../../../acquire")

# These layouts are used throughout the notebook for consistent sizing of objects
table_style = {'description_width': 'initial'}
table_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
date_layout = {'width': '275px', 'min_width': '200px', 'height': '28px', 'min_height': '28px'}
checkbox_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
statusbar_layout = {'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}
# login_text = HTML(value="<b>Please enter your username to be taken to a login page</b>")
# username_text = Text(value=None, placeholder="user", description="Username: ")
# login_button = Button(description="Login", button_style="success")
# status_text = HTML(value="")
# login_link_box = Output()

user = None

base_url = "https://hugs.acquire-aaai.com/t"
hugs_url = "https://hugs.acquire-aaai.com"

from HUGS.Interface import create_user, get_login

# Login box
# Load from credentials
reg_box = create_user()
user, login_box = get_login()

def parse_results(results):
    """ Split the keys into a list of each key and the date that the data covers
        
        Args:
            results (dict): Dictionary of search results
        Returns:
            list (tuple): List of date, data key list pairs
    """
    date_keys = {}
    for key in results.keys():
        keys = sorted(results[key])
        start_key = keys[0]
        end_key = keys[-1]
        # Get the first and last dates from the keys in the search results
        start_date = start_key.split("/")[-1].split("_")[0]
        end_date = end_key.split("/")[-1].split("_")[-1]

        dates_covered = start_date + "_" + end_date

        date_keys[key] = {"dates": dates_covered, "keys": keys}

    return date_keys


###############
# Search



date_keys = None


def call_search(x):
    """ Call the search function and pass it the values 
        in the text boxes
            
    """
    from datetime import datetime
    from Acquire.ObjectStore import datetime_to_string
    from HUGS.Client import Search
    start = datetime(1970, 1, 1)  # datetime.combine(start_picker.value, datetime.min.time())
    end = datetime.now()  # datetime.combine(end_picker.value, datetime.min.time())

    split_search_terms = search_terms.value.replace(" ", "").split(",")
    split_locations = locations.value.replace(" ", "").split(",")

    global search_results
    global date_keys

    search = Search(service_url=base_url)
    search_results = search.search(search_terms=split_search_terms, locations=split_locations, data_type=data_type.value, start_datetime=start, end_datetime=end)

    if search_results:
        date_keys = parse_results(search_results)
        status_box.value = f"<font color='green'>Success</font>"
        # Now we have search results we can select the ones we want to download
        create_download_box()
    else:
        status_box.value = f"<font color='red'>No results</font>"


search_results = None

search_terms = widgets.Text(value="", placeholder="Search", description="Search terms:", disabled=False)
locations = widgets.Text(value="", placeholder="BSD, HFD", description="Locations:", disabled=False)
data_type = widgets.Dropdown(options=["CRDS", "GC"], value="CRDS", description="Data type", disabled=False)

# search_layout = widgets.Layout(display = "flex", width = "50%")
search_button = widgets.Button(description="Search", button_style="success")
# layout=widgets.Layout(flex='1 1 0%', width='25%')


start_picker = widgets.DatePicker(description='Start date', disabled=False)
end_picker = widgets.DatePicker(description='End date', disabled=False)

status_box = widgets.HTML(value="")

search_children = [search_terms, locations, start_picker, end_picker, data_type,
                   search_button, status_box]
# search_box.layout = search_layout


search_button.on_click(call_search)
