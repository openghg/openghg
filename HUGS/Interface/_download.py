__all__ = ["download_box"]

import ipywidgets as widgets
from HUGS.Client import Retrieve
from pandas import read_json as pd_read_json

# These layouts are used throughout the notebook for consistent sizing of objects
table_style = {'description_width': 'initial'}
table_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
date_layout = {'width': '275px', 'min_width': '200px', 'height': '28px', 'min_height': '28px'}
checkbox_layout = {'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
statusbar_layout = {'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}

def download_box():
    """ Creates the plotting box that holds the plotting buttons and windows

        Returns:
            ipywidgets.VBox    
    """
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

    d_box = widgets.VBox(children=[header_box, dynamic_box, download_button_box, status_bar])

    # download_box = VBox[header_box, dynamic_box, download_button_box, status_bar]

    return d_box

    selected_data = []

    def select_data(**kwargs):
        selected_data.clear()

        for key in kwargs:
            if kwargs[key] is True:
                selected_data.append(key)

    def update_statusbar(text):
        status_bar.value = f"Status: {text}"

    data = None

    def download_data(a):
        """ Download the data in the selected keys from the object store

            Returns:
                Pandas.Dataframe of selected data
        """


        update_statusbar("Downloading...")

        download_keys = {key: date_keys[key]["keys"] for key in selected_data}
#         print(download_keys)

        retrieve = Retrieve(service_url=base_url)

        global data
        data = retrieve.retrieve(keys=download_keys)

        # Conver the JSON into Dataframes
        for key in data:
            data[key] = pd_read_json(data[key])

        # Update the status bar
        if data:
            update_statusbar("Retrieval complete")
            # Create the plotting box
            create_plotting_box()
        else:
            update_statusbar("No data downloaded")

    download_button.on_click(download_data)
    out = widgets.interactive_output(select_data, arg_dict)

#     display(complete, out)
