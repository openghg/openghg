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

# Workflow 1: handling overlapping observation data

+++

## 1. Overlapping observations

For some surface sites in the networks considered, there will be multiple independent measurements made for the various species at the same time. This is often due to a site having multiple inlets which allow gases to be sampled at different heights.

Often, when retrieving data for comparison there will be preferences between the streams of measurements, for example based on the inlet height. Within `openghg` it is possible to set a *ranking* for the different data sources so that the preferred data for a given species is always selected by default and without the need to know the exact details every time this data is accessed.

```{code-cell} ipython3
from openghg.retrieve import search_surface

search_surface(site="bsd", species="co")
```

Before we set any ranking for this data, we can still retrieve this from the object store using the `get_obs_surface` function, as we did before, but because there are multiple inlets we must specify these details to be able to return unambiguous observation data:

```{code-cell} ipython3
from openghg.retrieve import get_obs_surface

obs_data_42m = get_obs_surface(site="bsd", species="co", inlet="42m")
obs_data_108m = get_obs_surface(site="bsd", species="co", inlet="108m")
obs_data_248m = get_obs_surface(site="bsd", species="co", inlet="248m")

## Uncomment the cell below to see what happens if we don't include the inlet details.
# obs_data = get_obs_surface(site="bsd", species="co")
```

We can get around this by setting up ranking details once, which will then persist as long as the object store exists or this is updated again.

+++

## 2. Get ranking data

We can access the ranking data and see any pre-existing details associated with our data by using the `RankSources` class and the `get_sources()` method. In this case we are looking at the Bilsdale site ("BSD") and the carbon monoxide ("co") species:

```{code-cell} ipython3
from openghg.client import rank_sources

r = rank_sources(site="BSD", species="co")
```

What this tells us that, at the moment, there is no ranking data set for any of the inlets.
 - rank data for each inlet is set to 'NA' as shown by `{'rank_data':'NA',...}`

+++

## 3. Set ranking data

To set ranking data for carbon monoxide at Bilsdale for given date ranges we can use the `set_rank` method along with the relevant details for each inlet. Here we want to set the following for our data:

- From 01/01/2016 to 01/01/2018 (exclusive range)
  - Access carbon monoxide data from the inlet at "248m"
- From 01/01/2018 to 30/05/2019
  - Access carbon monoxide data from the inlet at "42m"
- From 30/05/2019 to 30/11/2021
  - Access carbon monoxide data from the inlet at "108m"

```{code-cell} ipython3
r.set_rank(inlet="248m", rank=1, start_date="2016-01-01", end_date="2018-01-01")
r.set_rank(inlet="42m", rank=1, start_date="2018-01-01", end_date="2019-05-30")
r.set_rank(inlet="108m", rank=1, start_date="2019-05-30", end_date="2021-11-30")
```

Secondary ranks (`rank=2`) and so forth can also be set covering the same date ranges, to set a preference order based on which data is available.

Now we can check everything was set correctlying using `get_sources` again as above:

```{code-cell} ipython3
r.get_sources(site="BSD", species="co")
```

## 4. Retrieve data

Once this has been set, we can now try retrieving this data from the object store again. Whereas before we had to specify an inlet, we can now rely on the highest ranked data always being returned for each date range:

```{code-cell} ipython3
co_data = get_obs_surface(site="bsd", species="co")
```

```{code-cell} ipython3
co_data.data
```

You can also see that because we extracted all the data for this site and species, this has also returned details of which inlet was applicable for each data point.

We will also have additional metadata associated with our returned `co_data` object which includes the details of the ranking and applicable date ranges:

```{code-cell} ipython3
co_data.metadata["rank_metadata"]
```
