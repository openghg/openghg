""" 
    A class to create the interface for HUGS
    
"""
import bqplot as bq
from Acquire.Client import User
import collections
from datetime import datetime
import functools
import ipywidgets as widgets
import ipyleaflet
from HUGS.Client import Retrieve, Search, Process
from HUGS.Interface import generate_password
import json
import numpy as np
import os
import pandas as pd
from pathlib import Path
import matplotlib
import random
import tempfile

__all__ = ["Interface"]

class Interface:
    """ 
        Handles the creation of an interface for the HUGS platform

        WIP

        Curently widgets will ? be stored in a defaultdict of lists so ipywidgets.VBoxes can
        be easily created from

        Each box creation function should return either an ipywidgets.VBox or HBox
        with a set layout

        Can move the layouts to be class members ? 
    """
    def __init__(self):
        self._base_url = "https://hugs.acquire-aaai.com/t"
        self._search_results = None
        # This is the order in which they'll be shown (if created)
        self._module_list = ["register", "login", "search", "selection", "download", 
                            "map", "plot_window", "plot_controls", "plot_complete", 
                            "status_bar", "upload"]
        # Maybe just made _widgets a defaultdict(list) as originally thought?
        self._widgets = collections.defaultdict(widgets.VBox)

        # Number of plots created
        self._n_figs = 0
        # User
        self._user = None

        self._to_plot = {}

        # Styles - maybe these can be moved somewhere else?
        self.table_style = {'description_width': 'initial'}
        self.table_layout = {'width': '100px', 'min_width': '100px','height': '28px', 'min_height': '28px'}
        self.date_layout = {'width': '275px', 'min_width': '200px','height': '28px', 'min_height': '28px'}
        self.checkbox_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
        self.statusbar_layout = {'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}
        self.small_button_layout = widgets.Layout(min_width='80px', max_width='100px')
        self.med_button_layout = widgets.Layout(min_width='80px', max_width='120px')

        self._spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

        self._plot_box = []

        params_file = (os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../Data/site_codes.json")
        with open(params_file, "r") as f:
            data = json.load(f)
            self._site_locations = data["locations"]
            # Keyed name: code
            self._site_codes = data["name_code"]
            # Keyed code: name
            self._site_names = data["code_name"]

        # colour_maps = (os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../Data/colour_maps.json")
        # with open(colour_maps, "r") as f:
        #     data = json.load(f)
        #     self._tableau20 = list(data["tableau20"].values())
        #     self._colour_blind = list(data["colour_blind"].values())

        # self._tableau20 = list(matplotlib.cm.tab20.colors)
        # self._colour_blind = list(data["colour_blind"].values())

        self._tableau10 = ['#4E79A7','#F28E2B','#E15759','#76B7B2','#59A14F'] #,'#EDC948','#B07AA1','#FF9DA7','#9C755F','#BAB0AC']
        self._tableau20 = [matplotlib.colors.rgb2hex(colour) for colour in matplotlib.cm.tab20.colors]
        self._colour_blind = self._tableau20

    def rand_colors(self, tab_set="10", colour_blind=False):
        """ Get a random set of colours

            Returns:
                list: A random list of colours
        """
        if tab_set == "10":
            colours = self._tableau10.copy()
        else:
            colours = self._tableau20.copy()

        # Need to copy as shuffle shuffles in-place
        if colour_blind:
            colours = self._colour_blind.copy()

        random.shuffle(colours)

        return colours

    def create_login(self, user, x):
        """ Create a basic login widget and add it to the widget dict

        """
        return False
    
    def create_registration_box(self):
        """
            User creation box

            Returns:
                ipywidgets.VBox
        """
        username_box = widgets.Text(value=None, placeholder="username", description="Username: ")
        suggested_password = widgets.Label(value=f"Suggested password : {generate_password()}")
        spacer = widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))
        password_box = widgets.Password(description="Password: ", placeholder="")
        conf_password_box = widgets.Password(description="Confirm: ", placeholder="")
        register_button = widgets.Button(description="Register", button_style="primary", layout=self.small_button_layout)
        
        status_text = "<font color='blue'>Enter credentials</font>"
        self.set_status(status_text=status_text)

        # self.set_status(status_text=f"<font color='blue'>Enter credentials</font>")
        output_box = widgets.Output()

        def register_user(a):
            if password_box.value != conf_password_box.value:
                password_match_error = f"<font color='red'>Passwords do not match</font>"
                self.set_status(status_text=password_match_error)
            else:
                result = User.register(username=username_box.value, password=password_box.value, identity_url=f"{self._base_url}/identity")

                with output_box:
                    display(result["qrcode"])
                
                qr_code_message = f"<font color='green'>Please scan QR code with authenticator app</font>"
                self.set_status(status_text=qr_code_message)

        register_button.on_click(register_user)

        return [username_box, suggested_password, password_box, conf_password_box, spacer, register_button, output_box]

    def create_login_box(self):
        """ Create a login box

            Returns:
                tuple (User, list): Acquire.User and a list of ipywidgets
        """
        login_text = widgets.HTML(value="<b>Please click the buton below to create a login link</b>")
        username_text = widgets.Text(value=None, placeholder="username", description="Username: ")
        status_text = widgets.HTML(value=f"<font color='black'>Waiting for login</font>")
        spacer = widgets.Text(value=None)
        spacer.layout.visibility = "hidden"
        login_button = widgets.Button(description="Login", button_style="success", layout=self.small_button_layout)
        # login_button_box = widgets.HBox(children=[login_button])
        # login_button_box.layout.object_position = "right"
        login_link_box = widgets.Output()

        def login(a):
            self._user = User(username=username_text.value, identity_url=f"{self._base_url}/identity")

            with login_link_box:
                # print(username_text.value)
                response = self._user.request_login()
                self._user.wait_for_login()

            # if user.wait_for_login():
            #     status_text.value = f"<font color='green'>Login success</font>"
            # else:
            #     status_text.value = f"<font color='red'>Login failure</font>"

        login_button.on_click(login)
        login_widgets = [username_text, spacer, login_button, status_text, login_link_box]

        return login_widgets

    def create_search_box(self):
        """ Create the searching interface

            Returns:
                list: List of ipywidgets widgets
        """
        search_terms = widgets.Text(value="", placeholder="Search", description="Search terms:", disabled=False)
        locations = widgets.Text(value="", placeholder="BSD, HFD", description="Locations:", disabled=False)
        data_type = widgets.Dropdown(options=["CRDS", "GC"], value="CRDS", description="Data type", disabled=False)
        search_button = widgets.Button(description="Search", button_style="success", layout=self.small_button_layout)
        start_picker = widgets.DatePicker(description='Start date', disabled=False)
        end_picker = widgets.DatePicker(description='End date', disabled=False)
        spacer = widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))
        status_box = widgets.HTML(value="")

        def call_search(x):
            if start_picker.value:
                start = datetime.combine(start_picker.value, datetime.min.time())
            else:
                start = datetime(2000, 1, 1)

            if end_picker.value:
                end = datetime.combine(end_picker.value, datetime.min.time())
            else:
                end = datetime.now()

            # Remove any spaces, split by comma
            split_search_terms = search_terms.value.replace(" ", "").split(",")
            split_locations = locations.value.replace(" ", "").split(",")

            search = Search(service_url=self._base_url)
            search_results = search.search(search_terms=split_search_terms, locations=split_locations,
                                                data_type=data_type.value, start_datetime=start, end_datetime=end)

            if search_results:
                # date_keys = self.parse_results(search_results=search_results)
                status_box_success = f"<font color='green'>Success</font>"
                self.set_status(status_text=status_box_success)
                # TODO - see how the layout works with voila for side-by-side list and map boxes
                self.add_widgets(section="selection", _widgets=self.create_selection_box(search_results=search_results))
                self.add_widgets(section="download", _widgets=self.create_download_box(search_results=search_results))
            else:
                status_box_error = f"<font color='red'>No results</font>"
                self.set_status(status_text=status_box_error)

        search_button.on_click(call_search)

        search_children = [search_terms, locations, start_picker, end_picker, data_type, spacer,
                           search_button, self._spacer]

        return search_children

    def create_selection_box(self, search_results):
        """ Select a list or map selection type

            Or create side by side option?

            Returns:
                list: List containing a HBox (WIP)
        """    
        split_button = widgets.Button(description="Split", button_style="info", layout=self.table_layout)
        list_button = widgets.Button(description="List selection", button_style="info", layout=self.table_layout)
        map_button = widgets.Button(description="Map selection", button_style="info", layout=self.table_layout)

        def split_click(a):
            self.add_widgets(section="download", _widgets=self.create_split_box(search_results=search_results))
            self.add_widgets(section="map", _widgets=widgets.VBox())

        def list_click(a):
            self.add_widgets(section="download", _widgets=self.create_download_box(search_results=search_results))
            self.add_widgets(section="map", _widgets=widgets.VBox())

        def map_click(a):
            self.add_widgets(section="download", _widgets=widgets.VBox())
            self.add_widgets(section="map", _widgets=self.create_map_box(search_results=search_results))

        split_button.on_click(split_click)
        list_button.on_click(list_click)
        map_button.on_click(map_click)
        
        buttons = [split_button, list_button, map_button]
        button_box = widgets.HBox(children=buttons)

        return [button_box]

    def create_split_box(self, search_results):
        """ Create a box with list selection on the left and map selection on the right

            Returns:
                list: List of HBox
        """
        list_widgets = self.create_download_box(search_results=search_results)
        map_widgets = self.create_map_box(search_results=search_results)

        combined = widgets.VBox(children=list_widgets+map_widgets)

        return combined

    def create_download_box(self, search_results):
        """ Creates the plotting box that holds the plotting buttons and windows
            
            Args:
                search_results (dict): Search results to be parsed for user information
            Returns:
                list: List of download widgets
        """
        header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=self.date_layout)
        # header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=self.table_layout)
        header_label_gas = widgets.HTML(value=f"<b>Gas</b>", layout=self.table_layout)
        header_label_dates = widgets.HTML(value=f"<b>Dates</b>", layout=self.date_layout)
        header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=self.checkbox_layout)

        checkbox_objects = []
        search_keys = []

        site_labels = []
        date_labels = []
        gas_labels = []

        # Pull this out to a function so we can use it in the plotting box as well

        # def create_data_selection(self, search_results, checkbox=True):
        #     """ Create the data selection widgets for downloading and plotting data

        #         Args:
        #             search_results (dict): Search results to parse
        #             checkbox (bool, default=True): Should checkboxes be ct
                

        #     """

        for key in search_results:
            # Create the checkboxes
            checkbox = widgets.Checkbox(value=False)
            checkbox_objects.append(checkbox)
            search_keys.append(key)

            # dates = date_keys[key]["dates"].replace("_", " to ").replace("T", " ")
            start_date = search_results[key]["start_date"]
            end_date = search_results[key]["end_date"]
            dates = f"{start_date} to {end_date}"

            date_label = widgets.Label(value=dates, layout=self.date_layout)

            split_key = key.split("_")
            site_name = split_key[0].upper()
            gas_name = split_key[1].upper()

            gas_label = widgets.Label(value=gas_name, layout=self.table_layout)
            site_text = f'{self._site_locations[site_name]["name"]} ({site_name})'
            site_label = widgets.Label(value=site_text, layout=self.table_layout)

            date_labels.append(date_label)
            site_labels.append(site_label)
            gas_labels.append(gas_label)

        arg_dict = {search_keys[i]: checkbox for i, checkbox in enumerate(checkbox_objects)}

        header_box = widgets.HBox(children=[header_label_site, header_label_gas, header_label_dates, header_label_select])

        site_vbox = widgets.VBox(children=site_labels)
        gas_vbox = widgets.VBox(children=gas_labels)
        dates_vbox = widgets.VBox(children=date_labels)
        checkbox_vbox = widgets.VBox(children=checkbox_objects)

        dynamic_box = widgets.HBox(children=[site_vbox, gas_vbox, dates_vbox, checkbox_vbox])

        select_all_button = widgets.Button(description="Select all", button_style="info", layout=self.table_layout)
        download_button = widgets.Button(description="Retrieve", button_style="success", layout=self.table_layout)

        # self._widgets["status_bar"] = widgets.HTML(value="Status: Waiting...", layout=statusbar_layout)
        # status_bar = self._widgets["status_bar"]
        # status_bar = widgets.HTML(value="Status: Waiting...", layout=self.statusbar_layout)
        self.set_status(status_text="Status: Waiting...")

        # self._status_bar = status_bar
        # self.add_widgets(section="download_status", _widgets=status_bar)

        def on_download_click(a):
            # download_keys = {key: search_results[key]["keys"] for key in arg_dict if arg_dict[key].value is True}
            # If the tickbox is ticked, copy the selected values from the search results to pass to the download fn            
            selected_results = {key: search_results[key] for key in arg_dict if arg_dict if arg_dict[key].value is True}
            self.download_data(selected_results=selected_results)

        def select_all(a):
            vals = [c.value for c in checkbox_objects]
            if all(vals):
                for c in checkbox_objects:
                    c.value = False
            else:
                for c in checkbox_objects:
                    c.value = True
                        
        
        download_button.on_click(on_download_click)
        select_all_button.on_click(select_all)

        download_button_box = widgets.VBox(children=[self._spacer, select_all_button, download_button])

        download_widgets = [header_box, dynamic_box, download_button_box]

        return download_widgets

    # Need to add this date parsing to the search_results
    def parse_results(self, search_results):
        """ Split the keys into a dictionary of each key and the date that the data covers
            
            Args:
                search_results (dict): Dictionary of search results
            Returns:
                dict: Keyed by object store key, each value being a dictionary of dates covered
                and the keys for these dates
        """
        date_keys = {}
        for key in search_results:
            keys = search_results[key]["keys"]
            start_date = search_results[key]["start_date"]
            end_date = search_results[key]["end_date"]
            # start_date, end_date = self.strip_dates_key(keys)
            dates_covered = start_date + "_" + end_date

            date_keys[key] = {"dates": dates_covered, "keys": keys}

        return date_keys

    # Moved to search function
    # def strip_dates_key(self, keys):
    #     """ Strips the date from a key, could this data just be read from JSON instead?
    #         Read dates covered from the Datasource?

    #         TODO - check if this is necessary - Datasource instead?

    #         Args:
    #             keys (list): List of keys containing data
    #             data/uuid/<uuid>/2014-01-30T10:52:30_2014-01-30T14:20:30'
    #         Returns:
    #             tuple (str,str): Start, end dates
    #     """
    #     if not isinstance(keys, list):
    #         keys = [keys]
        
    #     keys = sorted(keys)
    #     start_key = keys[0]
    #     end_key = keys[-1]
    #     # Get the first and last dates from the keys in the search results
    #     start_date = start_key.split("/")[-1].split("_")[0].replace("T", " ")
    #     end_date = end_key.split("/")[-1].split("_")[-1].replace("T", " ")

    #     return start_date, end_date

    def create_plotting_interface(self, selected_results, data, plot_number=1):
        """ Create the window for plotting the downloaded data

            Args:
                selected_results (dict): Dictionary of selected search results
                data (dict): Dictionary of data as JSON format
                plot_number (int, default=1): Plot number to add to figure
            Returns:
                list: List of widgets

            TODO
            1. Implement adding new plots to create a grid
            2. Editing of plots ?
            3. Adding new overlays to axes
            4. Updating axis titles, adding figure titles etc
        """
        # Create some checkboxes
        plot_checkboxes = []
        plot_keys = []

        site_labels = []
        date_labels = []
        gas_labels = []

        # TODO - pull this out into a function that can be used here and in create_download_box
        header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=self.table_layout)
        header_label_gas = widgets.HTML(value=f"<b>Gas</b>", layout=self.table_layout)
        header_label_dates = widgets.HTML(value=f"<b>Dates</b>", layout=self.date_layout)
        header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=self.checkbox_layout)

        for key in selected_results:
            start_date = selected_results[key]["start_date"]
            end_date = selected_results[key]["end_date"]
            dates = f"{start_date} to {end_date}"

            date_label = widgets.Label(value=dates, layout=self.date_layout)
    
            gas_name = selected_results[key]["metadata"]["species"]
            gas_label = widgets.Label(value=gas_name, layout=self.table_layout)
            
            site_code = selected_results[key]["metadata"]["site"]
            site_text = f'{self._site_locations[site_code.upper()]["name"]} ({site_code.upper()})'
            site_label = widgets.Label(value=site_text, layout=self.table_layout)

            plot_keys.append(key)
            plot_checkboxes.append(widgets.Checkbox(value=False))

            date_labels.append(date_label)
            site_labels.append(site_label)
            gas_labels.append(gas_label)

        select_instruction = widgets.HTML(value="<b>Select data: </b>", layout=self.table_layout)

        select_box = widgets.HBox(children=[select_instruction])

        header_box = widgets.HBox(children=[header_label_site, header_label_gas, header_label_dates, header_label_select])

        site_vbox = widgets.VBox(children=site_labels)
        gas_vbox = widgets.VBox(children=gas_labels)
        dates_vbox = widgets.VBox(children=date_labels)
        checkbox_vbox = widgets.VBox(children=plot_checkboxes)

        dynamic_box = widgets.HBox(children=[site_vbox, gas_vbox, dates_vbox, checkbox_vbox])

        # Edit box - for modifying plot types
        # Dropdown line/scatter
        # If line - linewidth
        # If scatter - marker size
        
        # Get the colors now so they don't change each time plot is clicked
        colors = self.rand_colors()
        # Enable/disable legend
        plot_dropdown = widgets.Dropdown(options=["Line", "Scatter"], value="Line", description="Plot style: ")
        marker_size = widgets.IntSlider(value=2, min=1, max=5, step=1, description="Linewidth: ")

        def on_plot_change(a):
            if plot_dropdown.value == "Line":
                marker_size.description = "Linewidth: "
            else:
                marker_size.description = "Marker size: "

        plot_dropdown.observe(on_plot_change)

        edit_box = widgets.HBox(children=[plot_dropdown, marker_size])

        # Plot button
        plot_button = widgets.Button(description="Plot", button_style="success", layout=self.table_layout)
        spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

        button_box = widgets.VBox(children=[plot_button, spacer, spacer])

        # ui_box = widgets.VBox(children=[horiz_select, plot_box])

        # Cleaner way of doing this?
        arg_dict = {plot_keys[i]: checkbox for i, checkbox in enumerate(plot_checkboxes)}

        # For some reason the datetimeindex of the dataframes isn't being preserved
        # and we're getting an unnamed column for the first column and a numbered index
        # But only on export of the dataframe to csv, otherwise just get the standard
        # 'co count', 'co stdev', 'co n_meas'

        def on_plot_clicked(a):
            # Get the data for ticked checkboxes
            to_plot = {key: data[key] for key in arg_dict if arg_dict[key].value is True}
            plot_data(to_plot=to_plot)

        # Create a dropdown to select which part of the dataframe
        # to plot, count, stddev etc

        def plot_data(to_plot):
            """ 
                Each key in the data dict is a dataframe
            """
            # Modify here so user can select line, scatter
            # Marker size etc
            plot_style = plot_dropdown.value

            x_scale = bq.DateScale()
            y_scale = bq.LinearScale()
            scales = {"x": x_scale, "y": y_scale}

            ax = bq.Axis(label="Date", scale=x_scale, grid_lines="none")
            ay = bq.Axis(label="Count", scale=y_scale, orientation="vertical", grid_lines="none")
            axes = [ax,ay]

            marks = []
            for key in to_plot:
                single_color = [random.choice(colors)]

                if plot_style == "Line":
                    mark = bq.Lines(scales=scales, colors=single_color, stroke_width=marker_size.value)
                else:
                    mark = bq.Scatter(scales=scales, colors=single_color, stroke_width=marker_size.value)
                    
                mark.x = to_plot[key].index
                # TODO - can modify this to be able to add error bars etc
                mark.y = to_plot[key].iloc[:,0]
                
                marks.append(mark)

            figure.marks = marks
            figure.axes = axes
  
        # lines = bq.Lines(x=np.arange(100), y=np.cumsum(np.random.randn(2, 100), axis=1), scales=scales)
        # lines = bq.Lines(scales=scales, colors=self.rand_colors())
        figure = bq.Figure(marks=[], axes=[], animation_duration=1000)
        figure.layout.width = "auto"
        figure.layout.height = "auto"
        figure.layout.min_height = "500px"

        spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

        plot_widgets = [header_box, dynamic_box, spacer, edit_box, figure, button_box]

        plot_button.on_click(on_plot_clicked)

        self._n_figs += 1

        # Need to update the plotting selection box each time download is called

        # Here have a function that just adds a whole new box to the box this has created
        # Have a VBox that contains the first, then update to append each plot to the grid
        # Each time we just append a new vbox to the list of boxes here

        # self._plot_box = [header_box, dynamic_box, figure, button_box]
        
        # # Initially just create a single plot
        # plot_box = widgets.VBox(children=plot_widgets)
        # self._plot_box.append(plot_box)

        return plot_widgets


    def plotting_interface(self, selected_results, data, plot_number=1):
        """ Create the window for plotting the downloaded data

            Args:
                selected_results (dict): Dictionary of selected search results
                data (dict): Dictionary of data as JSON format
                plot_number (int, default=1): Plot number to add to figure
            Returns:
                list: List of widgets

            TODO
            1. Implement adding new plots to create a grid
            2. Editing of plots ?
            3. Adding new overlays to axes
            4. Updating axis titles, adding figure titles etc
        """
        # Create some checkboxes
        plot_checkboxes = []
        plot_keys = []

        site_labels = []
        date_labels = []
        gas_labels = []

        box_layout = widgets.Layout(display='flex',
                                    flex_flow='column',
                                    align_items='flex-start',
                                    width='60%')

        select_layout = widgets.Layout(display='flex',
                            flex_flow='row',
                            align_items='flex-end',
                            width='98%')

        # TODO - pull this out into a function that can be used here and in create_download_box
        header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=box_layout)
        header_label_gas = widgets.HTML(value=f"<b>Species</b>", layout=box_layout)
        header_label_select = widgets.HTML(value=f"<b></b>", layout=box_layout)

        for key in selected_results:
            start_date = selected_results[key]["start_date"]
            end_date = selected_results[key]["end_date"]
            dates = f"{start_date} to {end_date}"

            gas_name = selected_results[key]["metadata"]["species"].upper()
            gas_label = widgets.Label(value=gas_name, layout=box_layout)

            site_code = selected_results[key]["metadata"]["site"]
            site_text = f'{self._site_locations[site_code.upper()]["name"]} ({site_code.upper()})'
            site_label = widgets.Label(value=site_text)

            plot_keys.append(key)
            plot_checkboxes.append(widgets.Checkbox(value=False))

            site_labels.append(site_label)
            gas_labels.append(gas_label)

        select_instruction = widgets.HTML(value="<b>Select data: </b>", layout=box_layout)

        header_box = widgets.HBox(children=[header_label_site, header_label_gas, header_label_select], layout=select_layout)

        site_vbox = widgets.VBox(children=site_labels, layout=box_layout)
        gas_vbox = widgets.VBox(children=gas_labels, layout=box_layout)
        checkbox_vbox = widgets.VBox(children=plot_checkboxes, layout=box_layout)

        dynamic_box = widgets.HBox(children=[site_vbox, gas_vbox, checkbox_vbox])

        # Edit box - for modifying plot types
        # Dropdown line/scatter
        # If line - linewidth
        # If scatter - marker size

        # Get the colors now so they don't change each time plot is clicked
        colors = self.rand_colors()
        # Enable/disable legend

        # Plot button
        plot_button = widgets.Button(description="Plot", button_style="success", layout=self.table_layout)
        spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

        button_box = widgets.VBox(children=[plot_button, spacer, spacer])

        # ui_box = widgets.VBox(children=[horiz_select, plot_box])

        # Cleaner way of doing this?
        arg_dict = {plot_keys[i]: checkbox for i, checkbox in enumerate(plot_checkboxes)}

        # For some reason the datetimeindex of the dataframes isn't being preserved
        # and we're getting an unnamed column for the first column and a numbered index
        # But only on export of the dataframe to csv, otherwise just get the standard
        # 'co count', 'co stdev', 'co n_meas'

        def on_plot_clicked(a):
            # Get the data for ticked checkboxes
            self._to_plot = {key: data[key] for key in arg_dict if arg_dict[key].value is True}
            plot_data(to_plot=self._to_plot)

        # Create a dropdown to select which part of the dataframe
        # to plot, count, stddev etc

        x_scale = bq.DateScale()
        y_scale = bq.LinearScale()
        scales = {"x": x_scale, "y": y_scale}

        ax = bq.Axis(label="Date", label_offset="50px", scale=x_scale, grid_lines="none")
        ay = bq.Axis(label="Count", label_offset="50px", scale=y_scale, orientation="vertical", grid_lines="none")
        axes = [ax, ay]

        def plot_data(to_plot):
            """ 
                Each key in the data dict is a dataframe
            """

            marks = []
            for i, key in enumerate(to_plot):
                data = to_plot[key]

                species = data.attrs["species"].upper()
                site_name = data.attrs["station_long_name"]

                # single_color = [random.choice(colors)]

                single_color = [self._tableau10[i]]

                mark = bq.Lines(scales=scales, colors=single_color, stroke_width=1, labels=[site_name], display_legend=True)

                mark.x = data.time
                # TODO - can modify this to be able to add error bars etc
                mark.y = data["ch4_count"]

                ay.label = f"{species} (nmol/mol)"

                marks.append(mark)

                start_date = pd.Timestamp(data["time"][0].values)
                end_date = pd.Timestamp(data["time"][-1].values)

                dates = pd.date_range(start_date, end_date, freq='M')
                options = [(date.strftime('%d %b %Y'), date) for date in dates]
                index = (0, len(options)-1)

                selection_range_slider.options = options
                selection_range_slider.index = index

            figure.marks = marks
            figure.axes = axes

        # lines = bq.Lines(x=np.arange(100), y=np.cumsum(np.random.randn(2, 100), axis=1), scales=scales)
        # lines = bq.Lines(scales=scales, colors=self.rand_colors())
        figure = bq.Figure(marks=[], axes=[], animation_duration=1000)
        figure.layout.width = "auto"
        figure.layout.height = "auto"
        figure.layout.min_height = "500px"

        spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

        # first_key = list(self._to_plot.keys())[0]
        # Timestamps for initial values
        start_date = pd.Timestamp("2012-01-01")
        end_date = pd.Timestamp("2019-01-01")

        dates = pd.date_range(start_date, end_date, freq='M')
        options = [(date.strftime('%d %b %Y'), date) for date in dates]
        index = (0, len(options)-1)

        selection_range_slider = widgets.SelectionRangeSlider(
                                        options=options,
                                        index=index,
                                        description='Select date',
                                        orientation='horizontal',
                                        layout={'width': '500px'}
                                    )
        def update_scale(dates):
            start, end = dates

            x_scale.min = start
            x_scale.max = end

        centre_layout = widgets.Layout(display='flex', flex_flow='column', align_items='center', width='100%')

        slider = widgets.interactive(update_scale, dates=selection_range_slider)

        figure_box = widgets.VBox(children=[figure, slider], layout=centre_layout)

        plot_widgets = [header_box, dynamic_box, figure_box, button_box]

        plot_box = widgets.VBox(children=plot_widgets)

        plot_button.on_click(on_plot_clicked)

        self._n_figs += 1

        # Need to update the plotting selection box each time download is called

        # Here have a function that just adds a whole new box to the box this has created
        # Have a VBox that contains the first, then update to append each plot to the grid
        # Each time we just append a new vbox to the list of boxes here

        # self._plot_box = [header_box, dynamic_box, figure, button_box]

        # # Initially just create a single plot
        # plot_box = widgets.VBox(children=plot_widgets)
        # self._plot_box.append(plot_box)

        return plot_box

    # def plotting_interface(self, selected_results, data, plot_number=1):
    #     """ Create the window for plotting the downloaded data

    #         Args:
    #             selected_results (dict): Dictionary of selected search results
    #             data (dict): Dictionary of data as JSON format
    #             plot_number (int, default=1): Plot number to add to figure
    #         Returns:
    #             list: List of widgets

    #         TODO
    #         1. Implement adding new plots to create a grid
    #         2. Editing of plots ?
    #         3. Adding new overlays to axes
    #         4. Updating axis titles, adding figure titles etc
    #     """
    #     # Create some checkboxes
    #     plot_checkboxes = []
    #     plot_keys = []

    #     site_labels = []
    #     date_labels = []
    #     gas_labels = []

    #     # TODO - pull this out into a function that can be used here and in create_download_box
    #     header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=self.table_layout)
    #     header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=self.checkbox_layout)

    #     site_data = collections.defaultdict(dict)
    #     for key in selected_results:
    #         species = selected_results[key]["metadata"]["species"].upper()
    #         site = selected_results[key]["metadata"]["site"].upper()

    #         if site not in site_data:
    #             site_data[site]["species_list"] = [species]
    #         else:
    #             site_data[site]["species_list"].append(species)

    #         # Key to access the data
    #         site_data[site][species] = key

    #     # Make the selectio menus
    #     selection_widgets = {}
    #     for site in site_data:
    #         site_text = f'{self._site_locations[site.upper()]["name"]} ({site.upper()})'
    #         site_label = widgets.Label(value=site_text)

    #         selection_box = widgets.Dropdown(
    #             options=site_data[site]["species_list"],
    #             value=site_data[site]["species_list"][0],
    #             description='Species',
    #             disabled=False
    #         )

    #         selection_widgets[site] = [site_label, selection_box]


    #     return selection_widgets

    #     select_instruction = widgets.HTML(value="<b>Select data: </b>", layout=self.table_layout)

    #     # Get the colors now so they don't change each time plot is clicked
    #     colors = self.rand_colors()
    #     # Enable/disable legend

    #     # Plot button
    #     plot_button = widgets.Button(description="Plot", button_style="success", layout=self.table_layout)
    #     spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

    #     button_box = widgets.VBox(children=[plot_button, spacer, spacer])

    #     def on_plot_clicked(a):
    #         keys = []
    #         for site in selection_widgets:
    #             species = 



    #         # Get the data for ticked checkboxes
    #         to_plot = {key: data[key] for key in site_data if arg_dict[key].value is True}
    #         plot_data(to_plot=to_plot)





    #     # For some reason the datetimeindex of the dataframes isn't being preserved
    #     # and we're getting an unnamed column for the first column and a numbered index
    #     # But only on export of the dataframe to csv, otherwise just get the standard
    #     # 'co count', 'co stdev', 'co n_meas'


    #     # Create a dropdown to select which part of the dataframe
    #     # to plot, count, stddev etc

    #     def plot_data(to_plot):
    #         """ 
    #             Each key in the data dict is a dataframe
    #         """
    #         # Modify here so user can select line, scatter
    #         # Marker size etc
    #         plot_style = plot_dropdown.value

    #         x_scale = bq.DateScale()
    #         y_scale = bq.LinearScale()
    #         scales = {"x": x_scale, "y": y_scale}

    #         ax = bq.Axis(label="Date", scale=x_scale, grid_lines="none")
    #         ay = bq.Axis(label="Count", scale=y_scale, orientation="vertical", grid_lines="none")
    #         axes = [ax, ay]

    #         marks = []
    #         for key in to_plot:
    #             single_color = [random.choice(colors)]

    #             if plot_style == "Line":
    #                 mark = bq.Lines(scales=scales, colors=single_color, stroke_width=marker_size.value)
    #             else:
    #                 mark = bq.Scatter(scales=scales, colors=single_color, stroke_width=marker_size.value)

    #             mark.x = to_plot[key].index
    #             # TODO - can modify this to be able to add error bars etc
    #             mark.y = to_plot[key].iloc[:, 0]

    #             marks.append(mark)

    #         figure.marks = marks
    #         figure.axes = axes

    #     # lines = bq.Lines(x=np.arange(100), y=np.cumsum(np.random.randn(2, 100), axis=1), scales=scales)
    #     # lines = bq.Lines(scales=scales, colors=self.rand_colors())
    #     figure = bq.Figure(marks=[], axes=[], animation_duration=1000)
    #     figure.layout.width = "auto"
    #     figure.layout.height = "auto"
    #     figure.layout.min_height = "500px"

    #     spacer = widgets.VBox(children=[widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))])

    #     plot_widgets = [header_box, dynamic_box, spacer, figure, button_box]

    #     plot_button.on_click(on_plot_clicked)

    #     self._n_figs += 1

    #     # Need to update the plotting selection box each time download is called

    #     # Here have a function that just adds a whole new box to the box this has created
    #     # Have a VBox that contains the first, then update to append each plot to the grid
    #     # Each time we just append a new vbox to the list of boxes here

    #     # self._plot_box = [header_box, dynamic_box, figure, button_box]

    #     # # Initially just create a single plot
    #     # plot_box = widgets.VBox(children=plot_widgets)
    #     # self._plot_box.append(plot_box)

    #     return plot_widgets

    def plot_controller(self, selected_results, data):
        """ This function controls the gridding of plots 
            and creates a new set of controlling widgets for each new plotting
            window

            Args:
                search_results (dict): Dictionary of search results
                data (dict): Dictionary of data downloaded from object store
            Returns:
                list: List of widgets to be added to widgets.VBox
        """
        # Clear the current plots
        self._plot_box.clear()

        # Add plot button can be here
        plot_window = widgets.VBox(children=self.create_plotting_box(selected_results=selected_results, data=data))
        add_button = widgets.Button(description="Add plot", button_style="primary", layout=self.table_layout)
        spacer = widgets.Text(value=None, layout=widgets.Layout(visibility="hidden"))
        # Add a number to the plot

        def add_window(a):
            # Number of figures so we know when to create a new HBox for gridding
            # Figure 1 is figure 0 here
            n_figs = self._n_figs - 1

            # Get current HBox
            new_plot = widgets.VBox(children=self.create_plotting_box(selected_results=selected_results, data=data))

            if n_figs % 2 == 0:
                current_box = self._plot_box[-1]
                # Take the current HBox at the last element and create a new list containing two HBoxes
                updated_box = widgets.HBox(children=[current_box, new_plot])
                self._plot_box[-1] = updated_box
            else:
                self._plot_box.append(new_plot)

            self.add_widgets(section="plot_window", _widgets=self._plot_box)


        self._plot_box.append(plot_window)

        self.add_widgets(section="plot_window", _widgets=self._plot_box)
        self.add_widgets(section="plot_controls", _widgets=[spacer, add_button])

        add_button.on_click(add_window)

        complete = [self._widgets["plot_window"], self._widgets["plot_controls"]]
        
        return complete
        

    def create_map_box(self, search_results):
        """ Create the mapping box for selection of site data from the map
            
            Args:
                search_results (dict): Dictionary containing search results
            Returns:
                list: List containing an ipyleaflet map (may be expanded to include
                other widgets)
        """
        # Parse the search results and extract dates, site locations etc
        site_locations = collections.defaultdict(dict)
        for key in search_results:
            # TODO - refactor this to work with the new search results
            # Key such as bsd_co, bsd_co2
            split_key = key.split("_")
            location = split_key[0]
            species = split_key[1]

            # start, end = self.strip_dates_key(search_results[key])
            start = search_results[key]["start_date"]
            end = search_results[key]["end_date"]
            # Need this in uppercase for use with the JSON
            location = location.upper()

            if location in site_locations:
                site_locations[location]["species"].append(species)
            else:
                site_data = self._site_locations[location]
                site_locations[location]["location"] = site_data["latitude"], site_data["longitude"]
                site_locations[location]["species"] = [species]
                site_locations[location]["name"] = site_data["name"]
                site_locations[location]["dates"] = f"{start} to {end}"

        # Now we can create the map with the data parsed from the search results
        center = [54.2361, -4.548]
        zoom = 5
        site_map = ipyleaflet.Map(center=center, zoom=zoom)
        
        # We only want to select each site once
        self._selected_sites = set()

        # These widgets are overlain on the map itself
        button_layout = widgets.Layout(min_width='80px', max_width='120px')
        download_layout = widgets.Layout(min_width='80px', max_width='100px')

        clear_button = widgets.Button(description="Clear selection", layout=button_layout)
        clear_control = ipyleaflet.WidgetControl(widget=clear_button, position='bottomleft')

        reset_button = widgets.Button(description="Reset map", layout=button_layout)
        reset_control = ipyleaflet.WidgetControl(widget=reset_button, position="bottomleft")

        selected_text = widgets.HTML(value="")
        selected_control = ipyleaflet.WidgetControl(widget=selected_text, position="topright")

        download_button = widgets.Button(description="Download", disabled=True, layout=download_layout)
        download_control = ipyleaflet.WidgetControl(widget=download_button, position="bottomright")

        site_map.add_control(clear_control)
        site_map.add_control(reset_control)
        site_map.add_control(selected_control)
        site_map.add_control(download_control)

        def site_select(r, **kwargs):
            self._selected_sites.add(r)
            selected_text.value = "Sites selected : " + ", ".join(list(self._selected_sites))
            download_button.disabled = False

        def clear_selection(a):
            self._selected_sites.clear()
            selected_text.value = ""
            download_button.disabled = True

        def reset_map(a):
            site_map.center = center
            site_map.zoom = 5

        def download_click(a):
            # Get the selected site's codes from the dict
            site_codes = {self._site_codes[s.lower()] for s in self._selected_sites}
            # If the site codes above are in the search_results' keys, add these to the dictionary
            to_download = {key: search_results[key] for key in search_results for s in site_codes if s.lower() in key}
            self.download_data(download_keys=to_download)

        # Create a marker for each site we have results for, on the marker show the
        # species etc we have data for
        for site in site_locations:
            site = site.upper()
            latitude, longitude = site_locations[site]["location"]
            name = site_locations[site]["name"]

            mark = ipyleaflet.Marker(location=(latitude, longitude), name=name, draggable=False)
            # These are added to the HTML in the popup of the marker
            species = ", ".join(site_locations[site]["species"])
            dates = site_locations[site]["dates"]

            html_string = "<br/>".join([(f"<b>{name}</b>"), "Species: ", species.upper(), "Data covering: ", dates])
            mark.popup = widgets.HTML(value=html_string)
            # We want to pass the name of the site selected to the site_select function
            mark.on_click(functools.partial(site_select, r=mark.name))

            site_map.add_layer(mark)

        reset_button.on_click(reset_map)
        clear_button.on_click(clear_selection)
        download_button.on_click(download_click)

        return [site_map]

    def create_fileselector(self, *args):
        header_layout = widgets.Layout(display='flex',flex_flow='row')

        table_layout = widgets.Layout(display='flex',flex_flow='row')

        main_layout = widgets.Layout(
            display='flex',
            flex_flow='row',
            align_items='stretch',
            width='100%',
            justify_content='space-between'
        )

        header_label_filename = widgets.HTML(value=f"<b>Filename</b>", layout=header_layout)
        header_label_datatype = widgets.HTML(value=f"<b>Data type</b>", layout=header_layout)
        header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=header_layout)

        filenames = []
        data_types = []
        checkboxes = []

        # This holds the binary data passed by the FileUpload widget
        data = {}

        data_type_list = ["CRDS", "GC"]

        files = ["file1 is really how fdfd",
                "file1 is really howfdfdf ", "file1 is really how"]

        def clear_checkboxes(*args):
            for c in checkboxes:
                c.value = False

        def upload_data(*args):
            # We want to add some metadata to the dictionary
            # to_upload = selection_button.value.copy()
            # Add in the data type of the 
            uuids = self.process_data(data=selection_button.value)

            with output:
                print(uuids)


        def update_filelist(*args):
            """ Update the list of files to be selected for upload
            
            """
            if self._user and self._user.is_logged_in():
                upload_button.disabled = False
                
            for filename in selection_button.value.keys():
                filenames.append(widgets.HTML(
                    value=f"{filename}", layout=widgets.Layout(flex='1 1 auto', width='auto')))
                data_types.append(widgets.Dropdown(options=data_type_list, value="GC",
                                                layout=widgets.Layout(flex='1 1 auto', width='auto')))
                checkboxes.append(widgets.Checkbox(
                    value=True, layout=widgets.Layout(flex='1 1 auto', width='auto')))

            filename_box = widgets.VBox(children=filenames)
            datatype_box = widgets.VBox(children=data_types)
            checkbox_box = widgets.VBox(children=checkboxes)

            list_box.children = [filename_box, datatype_box, checkbox_box]

        header = widgets.HBox(children=[header_label_filename, header_label_datatype, header_label_select], 
                                layout=main_layout)

        filename_box = widgets.VBox(children=filenames)
        datatype_box = widgets.VBox(children=data_types)
        checkbox_box = widgets.VBox(children=checkboxes)

        # list_box = widgets.HBox(children=[filename_box, datatype_box, checkbox_box], layout=main_layout)
        list_box = widgets.HBox(children=[], layout=main_layout)

        file_selector = widgets.VBox(children=[header, list_box])

        # Now create file selector button and clear, upload buttons

        selection_button = widgets.FileUpload(description="Select")
        clear_button = widgets.Button(button_style="danger", description="Clear")
        upload_button = widgets.Button(button_style="primary", description="Upload", disabled=True)

        # TODO - disable the upload button until the user is logged in
        selection_button.observe(update_filelist, names="value")

        clear_button.on_click(clear_checkboxes)
        upload_button.on_click(upload_data)

        button_box = widgets.VBox(children=[selection_button, widgets.HBox(children=[clear_button, upload_button])], 
                                    layout=main_layout)

        output = widgets.Output()
        output_box = widgets.VBox(children=[self._spacer, output])

        complete = widgets.VBox(children=[file_selector, button_box, output_box])

        return complete

    def process_data(self, data):
        """ Process the data passed by the FileUpload widget and 
            pass to the HUGS processing functions

            Args:
                user (Acquire User): Authorised User object
                data (dict): Dictionary created by FileUpload widget containing
                file metadata and binary content
            Returns:
                dict: Dictionary containing Datasource UUIDs for processed data
        """
        if self._user is None or not self._user.is_logged_in():
            # Raise or return a warning message here?
            raise ValueError("User must be logged in to upload data")

        # Create a process object to interact with HUGS
        process = Process(service_url=self._base_url)

        datasource_uuids = {}
        
        for filename in data:
            binary_content = data[filename]["content"]
            # Write data to temporary path for the moment
            with tempfile.TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir).joinpath(filename)
                with open(filepath, "wb") as f:
                    f.write(binary_content)
                    
                # Upload the file to
                uuids = process.process_files(user=self._user, files=filepath, data_type="CRDS")
                datasource_uuids.update(uuids)

        return datasource_uuids



    # Will this force an update ?
    def update_statusbar(self, status_name, text):

        # if not isinstance(self._widgets[status_name], widgets.widget_string.HTML):
        #     print(type(self._widgets[status_name]))
        #     raise TypeError("This function can only be used to update status bars that are ipywidgets HTML objects")
        return False
        # self._widgets[status_name].value = f"Status: {text}"

    def download_data(self, selected_results):
        """ Download the data in the selected keys from the object store

            Save the passed data as a temporary class member?

            Args:
                download_keys (dict): Dictionary 
            Returns:
                None
        """
        # Create a Retrieve object to interact with the HUGS Cloud object store
        retrieve = Retrieve(service_url=self._base_url)
        # Select the keys we want to download
        download_keys = {key: selected_results[key]["keys"] for key in selected_results}
        data = retrieve.retrieve(keys=download_keys)

        # TODO - get the status bar updates working
        # self.update_statusbar(status_name="download_status", text="Downloading...")

        # Update the status bar
        if data:
            # Convert the JSON into Dataframes
            for key in data:
                data[key] = pd.read_json(data[key])

            retrieve_success = f"<font color='green'>Retrieval complete</font>"
            self.set_status(status_text=retrieve_success)
            # Create the plotting box
            # self.add_widgets(section="plot_1", _widgets=self.create_plotting_box(selected_results=selected_results, data=data))
            # If we have new data clear the old plotting box
            # self.clear_widgets(section="plot_1")
            # Recreate the plot controller window
            self.add_widgets(section="plot_complete", _widgets=self.plot_controller(selected_results=selected_results, data=data))
        else:
            data_error = f"<font color='red'>No data downloaded</font>"
            self.set_status(status_text=data_error)

        return data

    def set_status(self, status_text):
        status_html = widgets.HTML(value=status_text)
        self._widgets["status_bar"].children = [status_html]


    def add_widgets(self, section, _widgets):
        """ Add widgets to be the children of the key section
            in the widget dictionary

            Args:
                section (str): Section / module of the GUI to assign these
                widgets to
                _widgets (widget / list): List of widgets
            Returns:    
                None
        """
        if not isinstance(_widgets, list):
            _widgets = [_widgets]

        self._widgets[section].children = _widgets

    def clear_widgets(self, section):
        """ Clear the widgets for this section by creating an empty
            VBox in their place

            Args:
                section (str): Name of section to clear
            Returns:
                None
        """
        self._widgets[section].children = []

    def show_module(self, module_name):
        """ Returns the widgets in a given module

            Args:
                module_name (str): Name of module from self._module_list 
            Returns:
                list: List containing ipywidgets VBox. The VBox is placed into a list
                as ipyvuetify requires the children attribute to be a list of widgets
        """
        module_name = module_name.lower()

        if module_name not in self._module_list:
            raise KeyError(f"{module_name} must be valid module from module list")

        return [self._widgets[module_name]]

    def voila_interface(self):
        """ Creates the interface for use with the voila interface

            Returns:
                None
        """
        self.add_widgets(section="register", _widgets=self.create_registration_box())
        self.add_widgets(section="login", _widgets=self.create_login_box())
        self.add_widgets(section="search", _widgets=self.create_search_box())
        self.add_widgets(section="upload", _widgets=self.create_fileselector())
        # self.add_widgets(section="status_bar", _widgets=[widgets.HTML(value="Woo")])


    def show_interface(self, new_user=False):
        """ Return the completed interface

            Returns:
                widgets.VBox
        """
        # Here we can assign children to the VBox's created in the function above
        if new_user:
            # widget_list.append(self.create_user())
            self.add_widgets(section="register", _widgets=self.create_registration_box())

        self.add_widgets(section="login", _widgets=self.create_login_box())

        self.add_widgets(section="search", _widgets=self.create_search_box())

        # box = widgets.VBox(children=list(self._widgets.values()))
        # This creates a list of of VBoxes to be placed in the main VBox
        boxes = [self._widgets[b] for b in self._module_list]
        box = widgets.VBox(children=boxes)
        
        return box



