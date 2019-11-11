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
from HUGS.Client import Retrieve, Search
from HUGS.Interface import generate_password
import json
import numpy as np
import os
import pandas as pd

__all__ = ["Interface"]

class Interface:
    """ 
        Handles the creation of an interface for the HUGS platform

        WIP

        Curently widgets will ? be stored in a defaultdict of lists so ipywidgets.VBoxes can
        be easily created from

        Each box creation function should return either an ipywidgets.VBox or HBox
        with a set layout

        Can move the layouts to be class members
    """
    def __init__(self):
        self._base_url = "https://hugs.acquire-aaai.com/t"
        self._search_results = None
        # This is the order in which they'll be shown (if created)
        self._module_list = ["register", "login", "search", "selection", "download", "map", "plot_1", "plot_2"]
        # Maybe just made _widgets a defaultdict(list) as originally thought?
        self._widgets = collections.defaultdict(widgets.VBox)

        # Styles - maybe these can be moved somewhere else?
        self.table_style = {'description_width': 'initial'}
        self.table_layout = {'width': '100px', 'min_width': '100px','height': '28px', 'min_height': '28px'}
        self.date_layout = {'width': '275px', 'min_width': '200px','height': '28px', 'min_height': '28px'}
        self.checkbox_layout = {'width': '100px', 'min_width': '100px','height': '28px', 'min_height': '28px'}
        self.statusbar_layout = {'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}
        # Lat/long of sites for use in map selection

        params_file = (os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../Data/site_codes.json")
        with open(params_file, "r") as f:
            data = json.load(f)
            self._site_locations = data["locations"]

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
        password_box = widgets.Password(description="Password: ", placeholder="")
        conf_password_box = widgets.Password(description="Confirm: ", placeholder="")
        register_button = widgets.Button(description="Register", button_style="primary")
        status_text = widgets.HTML(value=f"<font color='blue'>Enter credentials</font>")
        output_box = widgets.Output()

        def register_user(a):
            if password_box.value != conf_password_box.value:
                with output_box:
                    status_text.value = f"<font color='red'>Passwords do not match</font>"
            else:
                result = User.register(username=username_box.value, password=password_box.value, identity_url=f"{self._base_url}/identity")

                with output_box:
                    status_text.value = f"<font color='green'>Please scan QR code with authenticator app</font>"
                    display(result["qrcode"])

        register_button.on_click(register_user)

        return widgets.VBox(children=[username_box, suggested_password, password_box, conf_password_box, 
                                        register_button, status_text, output_box])

    def create_login_box(self):
        """ 
            Create a login box

            Returns:
                tuple (User, list): Acquire.User and a list of ipywidgets
        """
        login_text = widgets.HTML(value="<b>Please click the buton below to create a login link</b>")
        username_text = widgets.Text(value=None, placeholder="username", description="Username: ")
        status_text = widgets.HTML(value=f"<font color='black'>Waiting for login</font>")
        login_button = widgets.Button(description="Login", button_style="success")
        login_link_box = widgets.Output()

        user = None

        def login(a):
            global user
            user = User(username=username_text.value, identity_url=f"{self._base_url}/identity")

            with login_link_box:
                # print(username_text.value)
                response = user.request_login()

            # if user.wait_for_login():
            #     status_text.value = f"<font color='green'>Login success</font>"
            # else:
            #     status_text.value = f"<font color='red'>Login failure</font>"

        login_button.on_click(login)
        login_widgets = [username_text, login_button, status_text, login_link_box]
        # return user, widgets.VBox(children=[username_text, login_button, status_text, login_link_box])
        return user, login_widgets

    def create_search_box(self):
        """ Create the searching interface

            Returns:
                ipywidgets.VBox
        """
        search_terms = widgets.Text(value="", placeholder="Search", description="Search terms:", disabled=False)
        locations = widgets.Text(value="", placeholder="BSD, HFD", description="Locations:", disabled=False)
        data_type = widgets.Dropdown(options=["CRDS", "GC"], value="CRDS", description="Data type", disabled=False)
        search_button = widgets.Button(description="Search", button_style="success")

        start_picker = widgets.DatePicker(description='Start date', disabled=False)
        end_picker = widgets.DatePicker(description='End date', disabled=False)
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
                date_keys = self.parse_results(results=search_results)
                status_box.value = f"<font color='green'>Success</font>"
                # TODO - see how the layout works with voila for side-by-side list and map boxes
                self.add_widgets(section="selection", _widgets=self.create_selection_box(date_keys=date_keys, search_results=search_results))
                d_box = self.create_download_box(date_keys=date_keys)
                # Add the download widgets to the download box
                self.add_widgets(section="download", _widgets=self.create_download_box(date_keys=date_keys))

                # For now create the mapping box here
                # By default we'll create the list download box by default
                # OR have them side by side
                # self.add_widgets(section="map", _widgets=self.create_map_box(search_results=search_results))
            else:
                status_box.value = f"<font color='red'>No results</font>"

        search_button.on_click(call_search)

        search_children = [search_terms, locations, start_picker, end_picker, data_type,
                           search_button, status_box]

        return search_children

    def create_selection_box(self, date_keys, search_results):
        """ Select a list or map selection type

            Or create side by side option?

            Returns:
                list: List containing a HBox (WIP)
        """    
        # split_button = widgets.Button(description="Split", button_style="info", layout=self.table_layout)
        list_button = widgets.Button(description="List selection", button_style="info", layout=self.table_layout)
        map_button = widgets.Button(description="Map selection", button_style="info", layout=self.table_layout)

        def split_click(a):
            self.add_widgets(section="download", _widgets=self.create_split_box(date_keys=date_keys, search_results=search_results))
            self.add_widgets(section="map", _widgets=widgets.VBox())

        def list_click(a):
            self.add_widgets(section="download", _widgets=self.create_download_box(date_keys=date_keys))
            self.add_widgets(section="map", _widgets=widgets.VBox())

        def map_click(a):
            self.add_widgets(section="download", _widgets=widgets.VBox())
            self.add_widgets(section="map", _widgets=self.create_map_box(search_results=search_results))

        # split_button.on_click(split_click)
        list_button.on_click(list_click)
        map_button.on_click(map_click)
        
        buttons = [list_button, map_button]
        
        button_box = widgets.HBox(children=buttons)

        return [button_box]

    def create_split_box(self, date_keys, search_results):
        """ Create a box with list selection on the left and map selection on the right

            Returns:
                list: List of HBox
        """
        list_widgets = self.create_download_box(date_keys=date_keys)
        map_widgets = self.create_map_box(search_results=search_results)

        # list_box = widgets.VBox(children=list_widgets)
        # map_box = widgets.VBox(children=map_widgets)

        combined = widgets.HBox(children=list_widgets+map_widgets)

        return combined

    def create_download_box(self, date_keys):
        """ Creates the plotting box that holds the plotting buttons and windows
            
            Args:
                date_keys (dict): Dictionary of keys containing dates to be read
            Returns:
                list: List of download widgets
        """
        
        header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=self.table_layout)
        header_label_gas = widgets.HTML(value=f"<b>Gas</b>", layout=self.table_layout)
        header_label_dates = widgets.HTML(value=f"<b>Dates</b>", layout=self.date_layout)
        header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=self.checkbox_layout)

        checkbox_objects = []
        search_keys = []

        site_labels = []
        date_labels = []
        gas_labels = []
        for key in date_keys:
            # Create the checkboxes
            checkbox = widgets.Checkbox(value=False)
            checkbox_objects.append(checkbox)
            search_keys.append(key)

            dates = date_keys[key]["dates"].replace("_", " to ").replace("T", " ")
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

        download_button = widgets.Button(description="Retrieve", button_style="success", layout=self.table_layout)

        # self._widgets["status_bar"] = widgets.HTML(value="Status: Waiting...", layout=statusbar_layout)
        # status_bar = self._widgets["status_bar"]
        status_bar = widgets.HTML(value="Status: Waiting...", layout=self.statusbar_layout)
        # self._status_bar = status_bar
        self.add_widgets(section="download_status", _widgets=status_bar)

        def on_download_click(a):
            download_keys = {key: date_keys[key]["keys"] for key in arg_dict if arg_dict[key].value is True}
            self.download_data(download_keys=download_keys)

        download_button.on_click(on_download_click)
        download_button_box = widgets.HBox(children=[download_button])

        download_widgets = [header_box, dynamic_box, download_button_box, status_bar]

        return download_widgets

    def parse_results(self, results):
        """ Split the keys into a dictionary of each key and the date that the data covers
            
            Args:
                results (dict): Dictionary of search results
            Returns:
                dict: Keyed by object store key, each value being a dictionary of dates covered
                and the keys for these dates
        """
        # TODO - clear this up and improve comments
        date_keys = {}
        for key in results.keys():
            # keys = sorted(results[key])
            # start_key = keys[0]
            # end_key = keys[-1]
            # # Get the first and last dates from the keys in the search results
            # start_date = start_key.split("/")[-1].split("_")[0]
            # end_date = end_key.split("/")[-1].split("_")[-1]
            start_date, end_date = self.strip_dates_key(results[key])
            dates_covered = start_date + "_" + end_date

            date_keys[key] = {"dates": dates_covered, "keys": results[key]}

        return date_keys

    def strip_dates_key(self, keys):
        """ Strips the date from a key, could this data just be read from JSON instead?
            Read dates covered from the Datasource?

            TODO - check if this is necessary

            Args:
                keys (list): List of keys containing data
                data/uuid/<uuid>/2014-01-30T10:52:30_2014-01-30T14:20:30'
            Returns:
                tuple (str,str): Start, end dates
        """
        if not isinstance(keys, list):
            keys = [keys]
        
        keys = sorted(keys)
        start_key = keys[0]
        end_key = keys[-1]
        # Get the first and last dates from the keys in the search results
        start_date = start_key.split("/")[-1].split("_")[0].replace("T", " ")
        end_date = end_key.split("/")[-1].split("_")[-1].replace("T", " ")

        return start_date, end_date


    def create_plotting_box(self, data):
        """ Create the window for plotting the downloaded data

            TODO
            1. Implement adding new plots to create a grid
            2. Editing of plots ?
            3. Adding new overlays to axes
        """
        # Create some checkboxes
        plot_checkboxes = []
        plot_keys = []

        for key in data:
            # Create a more readable description
            desc = " ".join(key.split("_")).upper()
            plot_keys.append(key)
            plot_checkboxes.append(widgets.Checkbox(description=desc, value=False))

        select_instruction = widgets.HTML(value="<b>Select data: </b>", layout=self.table_layout)
        plot_button = widgets.Button(description="Plot", button_style="success", layout=self.table_layout)

        select_box = widgets.HBox(children=[select_instruction])
        checkbox_box = widgets.VBox(children=plot_checkboxes)
        horiz_select = widgets.HBox(children=[select_box, checkbox_box])
        plot_box = widgets.HBox(children=[plot_button])
        ui_box = widgets.VBox(children=[horiz_select, plot_box])

        # Cleaner way of doing this?
        arg_dict = {plot_keys[i]: checkbox for i, checkbox in enumerate(plot_checkboxes)}

        # For some reason the datetimeindex of the dataframes isn't being preserved
        # and we're getting an unnamed column for the first column and a numbered index
        # But only on export of the dataframe to csv, otherwise just get the standard
        # 'co count', 'co stdev', 'co n_meas'

        def on_plot_clicked(a):
            # Get the data for ticked checkboxes
            to_plot = {key: data[key] for key in arg_dict if arg_dict[key].value is True}
            # print([list(to_plot[k].columns) for k in to_plot])
            plot_data(to_plot=to_plot)

        # def select_data(**kwargs):
        #     selected_data.clear()

        #     for key in kwargs:
        #         if kwargs[key] is True:
        #             selected_data.append(key)
        output = widgets.Output()

        # Create a dropdown to select which part of the dataframe
        # to plot, count, stddev etc

        def plot_data(to_plot):
            """ 
                Each key in the data dict is a dataframe
            """
            # Here take the keys in the selected data list and use them to
            # access the Dataframes to plot
            # Use the same axes. Can have a button to create new plots etc in the future

            # TODO - change this to take the data directly from the dict?
            # plot_data = [data[x] for x in selected_data]
            # For now just plot the first column in the data

            # # Setup the axes
            # x_scale = bq.DateScale()
            # y_scale = bq.LinearScale()
            # scales = {"x": x_scale, "y": y_scale}

            lines.x = [to_plot[key].index for key in to_plot]
            lines.y = [to_plot[key].iloc[:,0] for key in to_plot]

        x_scale = bq.DateScale()
        y_scale = bq.LinearScale()
        scales = {"x": x_scale, "y": y_scale}

        ax = bq.Axis(label="Date", scale=x_scale)
        # TODO - this could be updated depending on what's being plot
        ay = bq.Axis(label="Count", scale=y_scale, orientation="vertical")

        # lines = bq.Lines(x=np.arange(100), y=np.cumsum(np.random.randn(2, 100), axis=1), scales=scales)
        lines = bq.Lines(scales=scales)
        figure = bq.Figure(marks=[lines], axes=[ax, ay], animation_duration=1000)

        plot_button.on_click(on_plot_clicked)

        return [ui_box, figure]

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
            # Key such as bsd_co, bsd_co2
            split_key = key.split("_")
            location = split_key[0]
            species = split_key[1]

            start, end = self.strip_dates_key(search_results[key])
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
        clear_button = widgets.Button(description="Clear selection")
        clear_control = ipyleaflet.WidgetControl(widget=clear_button, position='bottomright')

        reset_button = widgets.Button(description="Reset map")
        reset_control = ipyleaflet.WidgetControl(widget=reset_button, position="bottomright")

        selected_text = widgets.HTML(value="")
        selected_control = ipyleaflet.WidgetControl(widget=selected_text, position="topright")

        site_map.add_control(clear_control)
        site_map.add_control(reset_control)
        site_map.add_control(selected_control)

        # TODO - not sure how the selection of data using the map will work if there are multiple species

        def site_select(r, **kwargs):
            self._selected_sites.add(r)
            selected_text.value = "Sites selected : " + ", ".join(list(self._selected_sites))

        def clear_selection(a):
            self._selected_sites.clear()
            selected_text.value = ""

        def reset_map(a):
            site_map.center = center
            site_map.zoom = 5

        # Create a marker for each site we have results for, on the marker show the
        # species etc we have data for
        for site in site_locations:
            site = site.upper()
            latitude, longitude = site_locations[site]["location"]
            name = site_locations[site]["name"]

            mark = ipyleaflet.Marker(location=(latitude, longitude), name=name)
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

        return [site_map]

    # def get_locations(self, search_results):
    #     """ Returns the lat:long coordinates of the sites in the search results

    #         Returns:
    #             dict: Dictionary of site: latitude, longitude
    #     """
    #     # TODO - test for this so if change in key we get failure

    #     parsed = collections.defaultdict(dict)
    #     for key in search_results:
    #         # Key such as bsd_co, bsd_co2
    #         split_key = key.split("_")
    #         location = split_key[0]
    #         species = split_key[1]

    #         start, end = self.strip_dates_key(search_results[key])
    #         # Need this in uppercase for use with the JSON
    #         location = location.upper()

    #         if location in parsed:
    #             parsed[location]["species"].append(species)
    #         else:
    #             site_data = self._site_locations[location]
    #             parsed[location]["location"] = site_data["latitude"], site_data["longitude"]
    #             parsed[location]["species"] = [species]
    #             parsed[location]["name"] = site_data["name"]
    #             parsed[location]["dates"] = f"{start} to {end}" 

    #     return parsed

    # Will this force an update ?
    def update_statusbar(self, status_name, text):

        # if not isinstance(self._widgets[status_name], widgets.widget_string.HTML):
        #     print(type(self._widgets[status_name]))
        #     raise TypeError("This function can only be used to update status bars that are ipywidgets HTML objects")
        return False
        # self._widgets[status_name].value = f"Status: {text}"

    def download_data(self, download_keys):
        """ Download the data in the selected keys from the object store

            Args:
                date_keys (dict): Dictionary 
            Returns:
                None
        """
        retrieve = Retrieve(service_url=self._base_url)
        data = retrieve.retrieve(keys=download_keys)
        
        # TODO - get the status bar updates working
        # self.update_statusbar(status_name="download_status", text="Downloading...")

        # Convert the JSON into Dataframes
        for key in data:
            data[key] = pd.read_json(data[key])

        # Update the status bar
        if data:
            # update_statusbar("Download complete")
            # Create the plotting box
            self.add_widgets(section="plot_1", _widgets=self.create_plotting_box(data=data))
        else:
            raise NotImplementedError
            # update_statusbar("No data downloaded")


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

    def show_interface(self, new_user=False):
        """ Return the completed interface

        """
        # Here we can assign children to the VBox's created in the function above
        if new_user:
            # widget_list.append(self.create_user())
            self.add_widgets(section="register", _widgets=self.create_registration_box())

        user, login_box = self.create_login_box()
        self.add_widgets(section="login", _widgets=login_box)

        self.add_widgets(section="search", _widgets=self.create_search_box())

        # box = widgets.VBox(children=list(self._widgets.values()))
        # This creates a list of of VBoxes to be placed in the main VBox
        boxes = [self._widgets[b] for b in self._module_list]
        box = widgets.VBox(children=boxes)
        
        return box



