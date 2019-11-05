""" 
    A class to create the interface for HUGS
    
"""
import collections
from datetime import datetime
import ipywidgets as widgets
from HUGS.Interface import generate_password
from Acquire.Client import User

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
        self._widgets = {} #collections.defaultdict(list)

    def create_login(self, user, x):
        """ Create a basic login widget and add it to the widget dict

        """
        return False
    
    def create_user(self):
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

        base_url = "https://hugs.acquire-aaai.com/t"

        def register_user(a):
            if password_box.value != conf_password_box.value:
                with output_box:
                    status_text.value = f"<font color='red'>Passwords do not match</font>"
            else:
                result = User.register(username=username_box.value, password=password_box.value, identity_url=f"{base_url}/identity")

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
                tuple (User, ipywidgets.VBox): 
        """
        login_text = widgets.HTML(value="<b>Please click the buton below to create a login link</b>")
        username_text = widgets.Text(value=None, placeholder="username", description="Username: ")
        status_text = widgets.HTML(value=f"<font color='black'>Waiting for login</font>")
        login_button = widgets.Button(description="Login", button_style="success")
        login_link_box = widgets.Output()
        base_url = "https://hugs.acquire-aaai.com/t"

        user = None

        def login(a):
            global user
            user = User(username=username_text.value, identity_url=f"{base_url}/identity")

            with login_link_box:
                print(username_text.value)
                response = user.request_login()

            if user.wait_for_login():
                status_text.value = f"<font color='green'>Login success</font>"
            else:
                status_text.value = f"<font color='red'>Login failure</font>"

        login_button.on_click(login)
        return user, widgets.VBox(children=[username_text, login_button, status_text, login_link_box])

    def create_search_box(self):
        """ Create the searching interface

            Returns:
                ipywidgets.VBox
        """
        search_results = None
        date_keys = None

        search_terms = widgets.Text(value="", placeholder="Search", description="Search terms:", disabled=False)
        locations = widgets.Text(value="", placeholder="BSD, HFD", description="Locations:", disabled=False)
        data_type = widgets.Dropdown(options=["CRDS", "GC"], value="CRDS", description="Data type", disabled=False)
        search_button = widgets.Button(description="Search", button_style="success")

        start_picker = widgets.DatePicker(description='Start date', disabled=False)
        end_picker = widgets.DatePicker(description='End date', disabled=False)
        status_box = widgets.HTML(value="")

        search_children = [search_terms, locations, start_picker, end_picker, data_type,
                        search_button, status_box]

        search_vbox = widgets.VBox(children=search_children)

        date_keys = {"foo": 1, "bar": 2, "spam": 3}

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

            #search = Search(service_url=base_url)
            search_results = ["yah"]  # search.search(search_terms=split_search_terms, locations=split_locations,
            #          data_type=data_type.value, start_datetime=start, end_datetime=end)

            if search_results:
                # date_keys = self._parse_results(search_results)
                status_box.value = f"<font color='green'>Success</font>"
                # Now we have search results we can select the ones we want to download
                # TODO - move this so it's a part of the widgets dict
                d_box = self.create_download_box(date_keys=date_keys)
                # Update the children of the previous box to include the download box
                search_vbox.children = search_children + d_box
            else:
                status_box.value = f"<font color='red'>No results</font>"

        search_button.on_click(call_search)

        status_box.value = "Yahyah"



        

        # # From here create a
        # selected_data = []

        # # New from here
        # def select_data(**kwargs):
        #     selected_data.clear()
        #     for key in kwargs:
        #         if kwargs[key] is True:
        #             selected_data.append(key)

        # def update_statusbar(text):
        #     status_bar.value = f"Status: {text}"

        # data = None
        # def download_data(date_keys, selected_data):
        #     print("Yahyahyah")
        #     update_statusbar("Downloading...")

        #     download_keys = {key: date_keys[key]["keys"] for key in selected_data}

        #     retrieve = Retrieve(service_url=base_url)
        #     data = retrieve.retrieve(keys=download_keys)

        #     # Convert the JSON into Dataframes
        #     # TODO - the returned data will be an xarray Dataset in the future
        #     # Write a test to catch this change
        #     for key in data:
        #         data[key] = pd_read_json(data[key])

        #     # # Update the status bar
        #     # if data:
        #     #     update_statusbar("Download complete")
        #     #     # Create the plotting box
        #     #     create_plotting_box()
        #     # else:
        #     #     update_statusbar("No data downloaded")

        # # Here could update the status bar and call the download function
        # # download_button.on_click(download_data)

        # out = widgets.interactive_output(select_data, arg_dict)

        # return download_widgets

        return widgets.VBox(children=[search_vbox])
    
    def create_download_box(self, date_keys):
        """ Creates the plotting box that holds the plotting buttons and windows
            
            Args:
                date_keys (dict): Dictionary of keys containing dates to be read
            Returns:
                list: List of download widgets
        """
        table_style = {'description_width': 'initial'}
        table_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
        date_layout = {'width': '275px', 'min_width': '200px', 'height': '28px', 'min_height': '28px'}
        checkbox_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
        statusbar_layout = {'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}

        header_label_site = widgets.HTML(value=f"<b>Site</b>", layout=table_layout)
        header_label_gas = widgets.HTML(value=f"<b>Gas</b>", layout=table_layout)
        header_label_dates = widgets.HTML(value=f"<b>Dates</b>", layout=date_layout)
        header_label_select = widgets.HTML(value=f"<b>Select</b>", layout=checkbox_layout)

        checkbox_objects = []
        search_keys = []

        site_labels = []
        date_labels = []
        gas_labels = []
        for key in date_keys:
            # Create the checkboxes
            checkbox = widgets.Checkbox(value=False)
            checkbox_objects.append(checkbox)
            # search_keys.append(key)

            # dates = date_keys[key]["dates"].replace("_", " to ").replace("T", " ")
            # date_label = widgets.Label(value=dates, layout=date_layout)

            # split_key = key.split("_")
            # site_name = split_key[0].upper()
            # gas_name = split_key[1].upper()

            # gas_label = widgets.Label(value=gas_name, layout=table_layout)
            # site_label = widgets.Label(value=site_name, layout=table_layout)

            # date_labels.append(date_label)
            # site_labels.append(site_label)
            # gas_labels.append(gas_label)

        # arg_dict = {search_keys[i]: checkbox for i, checkbox in enumerate(checkbox_objects)}

        arg_dict_tmp = {chr(i+65): checkbox for i, checkbox in enumerate(checkbox_objects)}

        header_box = widgets.HBox(children=[header_label_site, header_label_gas, header_label_dates, header_label_select])

        site_vbox = widgets.VBox(children=site_labels)
        gas_vbox = widgets.VBox(children=gas_labels)
        dates_vbox = widgets.VBox(children=date_labels)
        checkbox_vbox = widgets.VBox(children=checkbox_objects)

        dynamic_box = widgets.HBox(children=[site_vbox, gas_vbox, dates_vbox, checkbox_vbox])

        download_button = widgets.Button(description="Download", button_style="success", layout=table_layout)

        # download_button.on_click(download_data)
        download_button_box = widgets.HBox(children=[download_button])

        status_bar = widgets.HTML(value="Status: Waiting...", layout=statusbar_layout)

        download_widgets = [header_box, dynamic_box, download_button_box, status_bar]

        # return widgets.VBox(children=download_widgets)
        return download_widgets

    def _parse_results(self, results):
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

    # Will this force an update ?
    def update_statusbar(self, text):
        self._widgets["status_bar"].value = f"Status: {text}"




    def show_interface(self, new_user=False):
        """ Return the completed interface

        """
        # widget_list = []

        if new_user:
            # widget_list.append(self.create_user())

            self._widgets["create"] = self.create_user()

        

        user, login_box = self.create_login_box()

        self._widgets["login_box"] = login_box

        self._widgets["search_box"] = self.create_search_box()

        # How best to get the dict to talk to each other?
        # Can refer to functions in the 

        box = widgets.VBox(children=list(self._widgets.values()))
        

        return box



