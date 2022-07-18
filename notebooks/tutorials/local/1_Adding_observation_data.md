---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.7
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Workflow 1: processing, searching and retrieving observations

This tutorial demonstrates how OpenGHG can be used to process new measurement data, search the data present and to retrieve this for analysis and visualisation.

+++

### Check installation

This tutorial assumes that you have installed `openghg`. To ensure install has been successful you can open an `ipython` console and try to import this module.

In a terminal type:

```bash
$ ipython
```

Then import `openghg` and print the version string associated with the version you have installed. If you get something like the below `openghg` is installed correctly.

```ipython
In [1]: import openghg
In [2]: openghg.__version__
Out[2]: '0.0.1'
```

If you get an ``ImportError`` please go back to the [install section of the documentation](https://docs.openghg.org/install.html).

### Jupyter notebooks

If you haven't used Jupyter notebooks before please see [this introduction](https://realpython.com/jupyter-notebook-introduction/).

+++

## 1. Setting up an object store

The OpenGHG platform uses what's called an *object store* to save data. Any saved data has been processed into a standardised format, assigned universally unique identifiers (UUIDs) and stored alongside associated metadata (such as site and species details). Storing data in this way allows for fast retrieval and efficient searching.

When using OpenGHG on a local machine the location of the object store is set using an `OPENGHG_PATH` environment variable (explained below) and this can be any directory on your local system.

For this tutorial, we will create a temporary object store which we can add data to. This path is fine for this purpose but as it is a temporary directory it may not survive a reboot of the computer.

The `OPENGHG_PATH` environment variable can be set up in the following way.

```{code-cell} ipython3
import os
import tempfile

tmp_dir = tempfile.TemporaryDirectory()
os.environ["OPENGHG_PATH"] = tmp_dir.name   # temporary directory

%load_ext autoreload
%autoreload 2
```

When creating your own longer term object store we recommend a path such as ``~/openghg_store`` which will create the object store in your home directory in a directory called ``openghg_store``. If you want this to be a permanent location this can be added to your "~/.bashrc" or "~/.bash_profile" file depending on the system being used. e.g. as

```bash
 export OPENGHG_PATH="$HOME/openghg_store"
```

+++

## 2. Adding and standardising data

+++

### Data types

Within OpenGHG there are several data types which can be processed and stored within the object store. This includes data from the AGAGE, DECC, NOAA, LondonGHG, BEAC2ON networks.

When uploading a new data file, the data type must be specified alongside some additional details so OpenGHG can recognise the format and the correct standardisation can occur. The details needed will vary by the type of data being uploaded but will often include the measurement reference (e.g. a site code) and the name of any network.

For the full list of accepted observation inputs and data types, there is a summary function which can be called:

```{code-cell} ipython3
from openghg.standardise import summary_data_types

summary = summary_data_types()

## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
# import pandas as pd; pd.set_option('display.max_rows', None)

summary
```

Note: there may be multiple data types applicable for a give site. This is can be dependent on various factors including the instrument type used to measure the data e.g. for Tacolneston ("TAC"):

```{code-cell} ipython3
summary[summary["Site code"] == "TAC"]
```

### DECC network

We will start by adding data to the object store from a surface site within the DECC network. Here we have accessed a subset of data from the Tacolneston site (site code "TAC") in the UK.

```{code-cell} ipython3
from openghg.util import retrieve_example_data

tac_data = retrieve_example_data(path="timeseries/tac_example.tar.gz")
```

As this data is measured in-situ, this is classed as a surface site and we need to use the `ObsSurface` class to interpret this data. We can pass our list of files to the `read_file` method associated within the `ObsSurface` class, also providing details on:
 - site code - `"TAC"` for Tacolneston
 - type of data we want to process, known as the data type - `"CRDS"`
 - network - `"DECC"`

This is shown below:

```{code-cell} ipython3
from openghg.client import standardise_surface

decc_results = standardise_surface(filepaths=tac_data, data_type="CRDS", site="TAC", network="DECC")
```

```{code-cell} ipython3
print(decc_results)
```

Here this extracts the data (and metadata) from the supplied files, standardises them and adds these to our created object store.

The returned `decc_results` will give us a dictionary of how the data has been stored. The data itself may have been split into different entries, each one stored with a unique ID (UUID). Each entry is known as a *Datasource* (see below for a note on Datasources). The `decc_results` output includes details of the processed data and tells us that the data has been stored correctly. This will also tell us if any errors have been encountered when trying to access and standardise this data.

+++

### AGAGE data

Another data type which can be added is data from the AGAGE network. The functions that process the AGAGE data expect data to have an accompanying precisions file. For each data file we create a tuple with the data filename and the precisions filename. *Note: A simpler method of uploading these file types is planned.*

+++

We can now retrieve the example data for Capegrim as we did above

```{code-cell} ipython3
capegrim_data = retrieve_example_data(path="timeseries/capegrim_example.tar.gz")
```

```{code-cell} ipython3
capegrim_data
```

We must create a `tuple` associated with each data file to link this to a precision file:

```python
list_of_tuples = [(data1_filepath, precision1_filepath), (data2_filepath, precision2_filepath), ...]
```

```{code-cell} ipython3
capegrim_data.sort()
capegrim_tuple = (capegrim_data[0], capegrim_data[1])
```

The data being uploaded here is from the Cape Grim station in Australia, site code "CGO".

+++

We can add these files to the object store in the same way as the DECC data by including the right keywords:
 - site code - `"CGO"` for Cape Grim
 - data type - `"GCWERKS"`
 - network - `"AGAGE"`

```{code-cell} ipython3
agage_results = standardise_surface(filepaths=capegrim_tuple, data_type="GCWERKS", site="CGO",
                              network="AGAGE", instrument="medusa")
```

When viewing `agage_results` there will be a large number of Datasource UUIDs shown due to the large number of gases in each data file

```{code-cell} ipython3
agage_results
```

#### A note on Datasources

Datasources are objects that are stored in the object store (++add link to object store notes++) that hold the data and metadata associated with each measurement we upload to the platform.

For example, if we upload a file that contains readings for three gas species from a single site at a specific inlet height OpenGHG    will assign this data to three different Datasources, one for each species. Metadata such as the site, inlet height, species, network etc are stored alongside the measurements for easy searching.

Datasources can also handle multiple versions of data from a single site, so if scales or other factors change multiple versions may be stored for easy future comparison.

+++

## 3. Searching for data

+++

### Visualising the object store

Now that we have added data to our created object store, we can view the objects within it in a simple force graph model. To do this we use the `view_store` function from the `objectstore` submodule. Note that the cell may take a few moments to load as the force graph is created.

In the force graph the central blue node is the `ObsSurface` node. Associated with this node are all the data processed by it. The next node in the topology are networks, shown in green. In the graph you will see `DECC` and `AGAGE` nodes from the data files we have added. From these you'll see site nodes in red and then individual datasources in orange.

+++

*Note: The object store visualisation created by this function is commented out here and won't be visible in the documentation but can be uncommented and run when you use the notebook version.*

```{code-cell} ipython3
from openghg.objectstore import visualise_store

# visualise_store()
```

Now we know we have this data in the object store we can search it and retrieve data from it.

+++

### Searching the object store

+++

We can search the object store by property using the `search(...)` function.

For example we can find all sites which have measurements for carbon tetrafluoride ("cf4") using the `species` keyword:

```{code-cell} ipython3
from openghg.client import search

search(species="cfc11")
```

We could also look for details of all the data measured at the Billsdale ("BSD") site using the `site` keyword:

```{code-cell} ipython3
search(site="tac")
```

For this site you can see this contains details of each of the species as well as the inlet heights these were measured at.

+++

### Quickly retrieve data

+++

Say we want to retrieve all the `co2` data from Tacolneston, we can perform perform a search and expect a [`SearchResults`](https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResult) object to be returned. If no results are found `None` is returned.

```{code-cell} ipython3
results = search(site="tac", species="co2")
```

```{code-cell} ipython3
results
```

We can retrive either some or all of the data easily using the `retrieve` function.

```{code-cell} ipython3
inlet_54m_data = results.retrieve(inlet="54m")
inlet_54m_data
```

Or we can retrieve all of the data and get a list of `ObsData` objects.

```{code-cell} ipython3
all_co2_data = results.retrieve_all()
```

```{code-cell} ipython3
all_co2_data
```

## 4. Retrieving data

+++

To retrieve the standardised data from the object store there are several functions we can use which depend on the type of data we want to access.

To access the surface data we have added so far we can use the `get_obs_surface` function and pass keywords for the site code, species and inlet height to retrieve our data.

In this case we want to extract the carbon dioxide ("co2") data from the Tacolneston data ("TAC") site measured at the "185m" inlet:

```{code-cell} ipython3
from openghg.client import get_obs_surface

co2_data = get_obs_surface(site="tac", species="co2", inlet="185m")
```

If we view our returned `obs_data` variable this will contain:

 - `data` - The standardised data (accessed using e.g. `obs_data.data`). This is returned as an [xarray Dataset](https://xarray.pydata.org/en/stable/generated/xarray.Dataset.html).
 - `metadata` - The associated metadata (accessed using e.g. `obs_data.metadata`).

```{code-cell} ipython3
co2_data
```

We can now make a simple plot using the `plot_timeseries` method of the `ObsData` object.

> **_NOTE:_**  the plot created below may not show up on the online documentation version of this notebook.

```{code-cell} ipython3
co2_data.plot_timeseries()
```

You can also pass any of `title`, `xlabel`, `ylabel` and `units` to the `plot_timeseries` function to modify the labels.

+++

#### Cleanup

+++

If you used the `tmp_dir` as a location for your object store at the start of the tutorial you can run the cell below to remove any files that were created to make sure any persistant data is refreshed when the notebook is re-run.

```{code-cell} ipython3
tmp_dir.cleanup()
```
