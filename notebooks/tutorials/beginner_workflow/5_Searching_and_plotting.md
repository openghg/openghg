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

# Workflow 5: Searching and plotting

In this short tutorial we'll show how to retrieve some data and create a simple plot using one of our plotting functions.

+++

As in the [previous tutorial](1_Adding_observation_data.ipynb), we will start by setting up our temporary object store for our data. If you've already create your own local object store you can skip the next few steps and move onto the **Searching** section.

```{code-cell} ipython3
import os
import tempfile

tmp_dir = tempfile.TemporaryDirectory()
os.environ["OPENGHG_PATH"] = tmp_dir.name   # temporary directory
```

```{code-cell} ipython3
from openghg.util import retrieve_example_data
from openghg.client import standardise_surface

tac_data = retrieve_example_data(path="timeseries/tac_example.tar.gz")
bsd_data = retrieve_example_data(path="timeseries/bsd_example.tar.gz")
```

```{code-cell} ipython3
standardise_surface(filepaths=tac_data, data_type="CRDS", site="TAC", network="DECC")
```

```{code-cell} ipython3
standardise_surface(filepaths=bsd_data, data_type="CRDS", site="BSD", network="DECC")
```

## Searching

+++

Let's search for all the methane data from Tacolneston

```{code-cell} ipython3
from openghg.client import search

ch4_results = search(site="tac", species="ch4")
ch4_results
```

If we want to take a look at the data from the 185m inlet we can first retrieve the data from the object store and then create a quick timeseries plot. See the [`SearchResults`](https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResults) object documentation for more information.

```{code-cell} ipython3
data_185m = ch4_results.retrieve(inlet="185m")
```

> **_NOTE:_**  the plots created below may not show up on the online documentation version of this notebook.

```{code-cell} ipython3
data_185m.plot_timeseries()
```

You can make some simple changes to the plot using arguments

```{code-cell} ipython3
data_185m.plot_timeseries(title="Methane at Tacolneston", xlabel="Time", ylabel="Conc.", units="ppm")
```

## Plot all the data

+++

We can also retrieve all the data, get a `list` of [`ObsData`](https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.ObsData) objects.

```{code-cell} ipython3
all_ch4_tac = ch4_results.retrieve_all()
```

Then we can use the `plot_timeseries` function from the `plotting` submodule to compare measurements from different inlets. This creates a [Plotly](https://plotly.com/python/) plot that should be interactive and and responsive, even with relatively large amounts of data.

```{code-cell} ipython3
from openghg.plotting import plot_timeseries

plot_timeseries(data=all_ch4_tac, units="ppb")
```

## Compare different sites

+++

We can easily compare data from different sites by doing a quick search to see what's available

```{code-cell} ipython3
ch4_data = search(species="ch4")
```

```{code-cell} ipython3
ch4_data
```

Then we refine our search to only retrieve the inlets we want

```{code-cell} ipython3
lower_inlets = search(species="ch4", inlet=["42m", "54m"])
```

```{code-cell} ipython3
lower_inlets
```

Then we can retrieve all the data and make a plot.

```{code-cell} ipython3
lower_inlet_data = lower_inlets.retrieve_all()
```

```{code-cell} ipython3
plot_timeseries(data=lower_inlet_data, title="Comparing CH4 measurements at Tacolneston and Bilsdale")
```

```{code-cell} ipython3
tmp_dir.cleanup()
```
