__all__ = ["search_emissions", "get_emissions_data", "search_box", "test_case"]

"""
    Searching interface functions
"""

from datetime import datetime
from Acquire.ObjectStore import datetime_to_string
from HUGS.Client import Search
# from HUGS.Interface import download_box
import ipywidgets as widgets


def search_emissions(search_terms):
    return ["WAO-20magl_EUROPE_201501", "WAO-20magl_EUROPE_201502", "WAO-20magl_EUROPE_201503", "WAO-20magl_EUROPE_201504"]

def get_emissions_data(search_terms):
    return []

def search_box():
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
    
    def call_search(x):
        if start_picker.value:
            start = datetime.combine(start_picker.value, datetime.min.time())
        else:
            start = datetime(2000,1,1)

        if end_picker.value:
            end = datetime.combine(end_picker.value, datetime.min.time())
        else:
            end = datetime.now()

        # Remove any spaces, split by comma
        split_search_terms = search_terms.value.replace(" ", "").split(",")
        split_locations = locations.value.replace(" ", "").split(",")

        #search = Search(service_url=base_url)
        search_results = ["yah"] #search.search(search_terms=split_search_terms, locations=split_locations, 
                                  #          data_type=data_type.value, start_datetime=start, end_datetime=end)

        if search_results:
            # date_keys = _parse_results(search_results)
            status_box.value = f"<font color='green'>Success</font>"
            # Now we have search results we can select the ones we want to download
            dbox = download_box()
            # Vertical spacer ? 
            # This breaks stuff here - find out why!
            # extra_vbox = [widgets.VBox(children=[widgets.HTML(value="YAHYAHYAH")])]
            yahyah = widgets.HTML(value="YAHYAHYAH")
            # search_vbox.children = [widgets.VBox(children=search_children.extend([yahyah]))]
            # updated = search_children + [yahyah]
            updated = search_children + dbox

            # updated_box = widgets.VBox(children=updated)

            search_vbox.children = updated
            
            # for f in search_children:
            #     print(type(f))
            # new_box = [search_vbox, extra_vbox]
            # search_vbox.children = extra_vbox
            # search_vbox.children = new_box
            # search_children.append(dbox)
        else:
            status_box.value = f"<font color='red'>No results</font>"
    
    search_button.on_click(call_search)

    return widgets.VBox(children=[search_vbox])


def _parse_results(results):
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

def test_case():
    from ipywidgets import Button, Checkbox, VBox
    cb1 = Checkbox(description='1')
    cb2 = Checkbox(description='2')
    cb3 = Checkbox(description='3')

    vb = VBox(children=[cb1, cb2, cb3])
    top_toggle = Checkbox(description='Remove 3')
    # show_results_button = ipywidgets.


    def remove_3(button):
        if button['new']:
            vb.children = [cb1, cb2]
        else:
            vb.children = [cb1, cb2, cb3]

    # here instead of having a toggle just have a button

    top_toggle.observe(remove_3, names='value')
    return VBox(children=[top_toggle, vb])


def download_box():
    """ Creates the plotting box that holds the plotting buttons and windows

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
    # for key in date_keys:
    #     # Create the checkboxes
    #     checkbox = widgets.Checkbox(value=False)
    #     checkbox_objects.append(checkbox)
    #     search_keys.append(key)

    #     dates = date_keys[key]["dates"].replace("_", " to ").replace("T", " ")
    #     date_label = widgets.Label(value=dates, layout=date_layout)

    #     split_key = key.split("_")
    #     site_name = split_key[0].upper()
    #     gas_name = split_key[1].upper()

    #     gas_label = widgets.Label(value=gas_name, layout=table_layout)
    #     site_label = widgets.Label(value=site_name, layout=table_layout)

    #     date_labels.append(date_label)
    #     site_labels.append(site_label)
    #     gas_labels.append(gas_label)

    # arg_dict = {search_keys[i]: checkbox for i, checkbox in enumerate(checkbox_objects)}

    header_box = widgets.HBox(children=[header_label_site, header_label_gas, header_label_dates, header_label_select])

    site_vbox = widgets.VBox(children=site_labels)
    gas_vbox = widgets.VBox(children=gas_labels)
    dates_vbox = widgets.VBox(children=date_labels)
    checkbox_vbox = widgets.VBox(children=checkbox_objects)

    dynamic_box = widgets.HBox(children=[site_vbox, gas_vbox, dates_vbox, checkbox_vbox])

    download_button = widgets.Button(description="Download", button_style="success", layout=table_layout)
    download_button_box = widgets.HBox(children=[download_button])

    status_bar = widgets.HTML(value="Status: Waiting...", layout=statusbar_layout)

    # d_box = widgets.VBox(children=[])
    download_widgets = [header_box, dynamic_box, download_button_box, status_bar]
    # download_box = VBox[header_box, dynamic_box, download_button_box, status_bar]

    return download_widgets
