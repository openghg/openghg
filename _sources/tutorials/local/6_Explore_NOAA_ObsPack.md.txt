---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Workflow 6: Explore NOAA ObsPack

+++

The NOAA ObsPack products are collections of observation data from many sites which have been collated and standardised. ObsPack data products are prepared by NOAA in consultation with data providers. Available ObsPack products can be accessed and downlaoded from the [ObsPack download](https://gml.noaa.gov/ccgg/obspack/data.php) page.

In this tutorial, we will demonstrate how the NOAA ObsPack can be loaded into the object store, explored and plotted.

+++

## 1. Loading the NOAA ObsPack data

```{code-cell} ipython3
import os
import tempfile

tmp_dir = tempfile.TemporaryDirectory()
os.environ["OPENGHG_PATH"] = tmp_dir.name   # temporary directory

%load_ext autoreload
%autoreload 2
```

As the ObsPack products contain a large amount of data we'll use the temporary directory we've created for the object store above to download and unpack the data. If you're using a different path for the object store you can uncomment the cell below and use that directory instead. Remember to use `tmp_download.cleanup()` if you do.

```{code-cell} ipython3
# tmp_download = tempfile.TemporaryDirectory()
# tmp_download_path = tmp_download.name
```

### Download the data

+++

For convenience we have included a copy of the "obspack_ch4_1_GLOBALVIEWplus_v2.0_2020-04-24" to be retrieved from our example database. Once this has been downloaded, this can be processd using the `add_noaa_obspack()` function available from `openghg.store` subpackage. The file is ~ 128 MB in size so might take a short time to download depending on your internet connection.

```{code-cell} ipython3
from openghg.util import retrieve_example_data
from openghg.store import add_noaa_obspack

noaa_obspack_directory = retrieve_example_data(path="obspack/obspack_ch4_example.tar.gz",
                                                download_dir=tmp_dir.name)
```

### Process and store the data

```{code-cell} ipython3
res = add_noaa_obspack(noaa_obspack_directory)
```

### Visualise the data within the object store

+++

The object store should now contain a larage amount of CH4, we can visualise the structure of the store like so:

```{code-cell} ipython3
from openghg.objectstore import visualise_store

visualise_store()
```

## 2. Search, retrieve and plot

Now we can query the object store and find all the flask data for example

```{code-cell} ipython3
from openghg.retrieve import search_surface

search_surface(species="ch4", measurement_type="flask", data_source="noaa_obspack")
```

Or we can do an all in one search and retrieve using `get_obs_surface`. Here we find CH4 data from Estevan Point, British Columbia, retrieve it and plot it.

```{code-cell} ipython3
from openghg.retrieve import get_obs_surface

data = get_obs_surface(site="HPB", species="ch4")
```

As there isn't any ranking data set (see tutorial 2) `get_obs_surface` doesn't know which inlet to select, we need to tell it.

```{code-cell} ipython3
data = get_obs_surface(site="HPB", species="ch4", inlet="93m")
```

```{code-cell} ipython3
data.plot_timeseries()
```
