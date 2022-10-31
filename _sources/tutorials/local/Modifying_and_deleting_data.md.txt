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

# Modifying and deleting data

+++

Sometimes you might want to modify some metadata after running the data through the standardisation scripts.
After the standardisation process the metadata associated with some data can still be edited. This can save
time if the data standardisation process is quite time consuming. Data can also be deleted from the object store.

```{code-cell} ipython3
from openghg.store import data_handler_lookup
from openghg.tutorial import populate_footprint_inert
```

We'll first add some footprint data to the object store.

```{code-cell} ipython3
populate_footprint_inert()
```

```{code-cell} ipython3
result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

```{code-cell} ipython3
result.metadata
```

We want to update the model name so we'll use the ``update_metadata`` method of the ``DataHandler`` object. To do this we need to take the
UUID of the Datasource returned by the ``data_handler_lookup`` function, this is the key of the metadata dictionary.

+++

> **_NOTE:_**  The UUID below will be different on your computer. Take the UUID from the metadata dictionary.

```{code-cell} ipython3
uuid = "b2177d42-9df9-4a08-b50b-8aadbd7fb9d7"
```

```{code-cell} ipython3
updated = {"model": "new_model"}

result.update_metadata(uuid=uuid, to_update=updated)
```

To confirm the metadata has been updated we can run the lookup data

```{code-cell} ipython3
new_result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

```{code-cell} ipython3
metadata = new_result.metadata[uuid]
```

And check the model has been changed.

```{code-cell} ipython3
metadata["model"]
```

## Deleting keys

+++

Let's accidentally add too much metadata for the footprint and then delete.

```{code-cell} ipython3
excess_metadata = {"useless_key": "useless_value"}
new_result.update_metadata(uuid=uuid, to_update=excess_metadata)
```

Oh no! We've added some useless metadata, let's remove it.

```{code-cell} ipython3
to_delete = ["useless_key"]
new_result.update_metadata(uuid=uuid, to_delete=to_delete)
```

```{code-cell} ipython3
result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

And check if the key is in the metadata:

```{code-cell} ipython3
"useless_key" in result.metadata[uuid]
```

# Deleting data

+++

To remove data from the object store we use `data_handler_lookup` again

```{code-cell} ipython3
result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

```{code-cell} ipython3
result.metadata
```

Each key of the metadata dictionary is a Datasource UUID. Please make sure that you double check the UUID of the Datasource you want to delete, this operation cannot be undone! Again, remember to change the UUID.

```{code-cell} ipython3
uuid = "b2177d42-9df9-4a08-b50b-8aadbd7fb9d7"
```

```{code-cell} ipython3
result.delete_datasource(uuid=uuid)
```

To make sure it's gone let's run the search again

```{code-cell} ipython3
result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

```{code-cell} ipython3
result.metadata
```

An empty dictionary means no results, the deletion worked.
