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

# Workflow 4: Searching and plotting

In this short tutorial we'll show how to retrieve some data and create a simple plot using one of our plotting functions.

+++

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
