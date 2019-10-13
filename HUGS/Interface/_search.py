from HUGS.Processing import search

table_style = {'description_width': 'initial'}
table_layout = {'width': '100px', 'min_width': '100px',
                'height': '28px', 'min_height': '28px'}
date_layout = {'width': '275px', 'min_width': '200px',
               'height': '28px', 'min_height': '28px'}
checkbox_layout = {'width': '100px', 'min_width': '100px',
                   'height': '28px', 'min_height': '28px'}
statusbar_layout = {'width': '250px', 'min_width': '250px',
                    'height': '28px', 'min_height': '28px'}

box_layout = {'border': '1px solid grey'}
download_box = VBox(children=[], layout=box_layout)
plotting_box = VBox(children=[], layout=box_layout)

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

date_keys = None

def call_search(x):
    """ Call the search function and pass it the values 
        in the text boxes
            
    """
    from datetime import datetime
    from Acquire.ObjectStore import datetime_to_string
    from HUGS.Client import Search
    # datetime.combine(start_picker.value, datetime.min.time())
    start = datetime(1970, 1, 1)
    end = datetime.now()  # datetime.combine(end_picker.value, datetime.min.time())

    split_search_terms = search_terms.value.replace(" ", "").split(",")
    split_locations = locations.value.replace(" ", "").split(",")

    global search_results
    global date_keys

    search = Search(service_url=base_url)
    search_results = search.search(search_terms=split_search_terms, locations=split_locations,
                                   data_type=data_type.value, start_datetime=start, end_datetime=end)

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
data_type = widgets.Dropdown(options=["CRDS", "GC"], vafrom HUGS.Processing import search

table_style={'description_width': 'initial'}
table_layout={'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
    
date_layout={'width': '275px', 'min_width': '200px', 'height': '28px', 'min_height': '28px'}
checkbox_layout={'width': '100px', 'min_width': '100px', 'height': '28px', 'min_height': '28px'}
statusbar_layout={'width': '250px', 'min_width': '250px', 'height': '28px', 'min_height': '28px'}

box_layout={'border': '1px solid grey'}
download_box=VBox(children=[], layout=box_layout)
plotting_box=VBox(children=[], layout=box_layout)

def parse_results(results):
    """ Split the keys into a list of each key and the date that the data covers

        Args:
            results (dict): Dictionary of search results
        Returns:
            list (tuple): List of date, data key list pairs
    """
    date_keys={}
    for key in results.keys():
    keys=sorted(results[key])
    start_key=keys[0]
    end_key=keys[-1]
    # Get the first and last dates from the keys in the search results
    start_date=start_key.split("/")[-1].split("_")[0]
    end_date=end_key.split("/")[-1].split("_")[-1]

    dates_covered=start_date + "_" + end_date

    date_keys[key]={
        "dates": dates_covered, "keys": keys}

    return date_keys

    date_keys=None

def call_search(x):
    """ Call the search function and pass it the values
        in the text boxes

    """
    from datetime import datetime
    from Acquire.ObjectStore import datetime_to_string
    from HUGS.Client import Search
    # datetime.combine(start_picker.value, datetime.min.time())
    start=datetime(1970, 1, 1)
    end=datetime.now()  # datetime.combine(end_picker.value, datetime.min.time())

    split_search_terms=search_terms.value.replace(" ", "").split(",")
    split_locations=locations.value.replace(" ", "").split(",")

    global search_results
    global date_keys

    search=Search(service_url=base_url)
    search_results=search.search(search_terms=split_search_terms, locations=split_locations,
                                 data_type=data_type.value, start_datetime=start, end_datetime=end)

    if search_results:
        date_keys=parse_results(search_results)
        status_box.value=f"<font color='green'>Success</font>"
        # Now we have search results we can select the ones we want to download
        create_download_box()
    else:
        status_box.value=f"<font color='red'>No results</font>"

search_results=None

search_terms=widgets.Text(value="", placeholder="Search", description="Search terms:", disabled=False)
locations=widgets.Text(value="", placeholder="BSD, HFD",description="Locations:", disabled=False)
data_type=widgets.Dropdown(options=["CRDS", "GC"], value="CRDS", description="Data type", disabled=False)

# search_layout = widgets.Layout(display = "flex", width = "50%")
search_button=widgets.Button(description="Search", button_style="success")
# layout=widgets.Layout(flex='1 1 0%', width='25%')
start_picker=widgets.DatePicker(description='Start date', disabled=False)
end_picker=widgets.DatePicker(description='End date', disabled=False)

status_box=widgets.HTML(value="")

search_children=[search_terms, locations, start_picker, end_picker, data_type,
                    search_button, status_box]
# search_box.layout = search_layout
search_button.on_click(call_search)

search_box=VBox(children=search_children)
search_box
