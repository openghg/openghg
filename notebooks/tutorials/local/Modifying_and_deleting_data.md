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
from openghg.tutorial import populate_footprint_inert, clear_tutorial_store, tutorial_store_path
```

```{code-cell} ipython3
tutorial_store_path()
```

We'll first add some footprint data to the object store.

```{code-cell} ipython3
clear_tutorial_store()
```

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
uuid = "60d68d7b-7d13-4b1d-8c78-237f9f7a0dea"
```

```{code-cell} ipython3
updated = {"model": "new_model"}

result.update_metadata(uuid=uuid, to_update=updated)
```

When you run `update_metadata` the internal store of metadata for each `Datasource` is updated. If you want to **really** make sure that the metadata in the object store has been updated you can run `refresh`.

```{code-cell} ipython3
result.refresh()
```

```{code-cell} ipython3
metadata = result.metadata[uuid]
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

# Restore from backup

If you've accidentally pushed some bad metadata you can fix this easily by restoring from backup. Each `DataHandler` object stores a backup of the current metadata each time you run `update_metadata`. Let's add some bad metadata, have a quick look at the backup and then restore it.

```{code-cell} ipython3
result = data_handler_lookup(data_type="footprints", site="TAC", height="100m")
```

```{code-cell} ipython3
result.metadata.keys()
```

```{code-cell} ipython3
uuid = "b703e490-2fdd-4bb3-bb16-66673673bf16"
```

```{code-cell} ipython3
bad_metadata = {"domain": "neptune"}
```

```{code-cell} ipython3
result.update_metadata(uuid=uuid, to_update=bad_metadata)
```

```{code-cell} ipython3
result.metadata[uuid]["domain"]
```

```{code-cell} ipython3
result.view_backup()
```

```{code-cell} ipython3
result.restore(uuid=uuid)
```

```{code-cell} ipython3
result.metadata[uuid]["domain"]
```

```{code-cell} ipython3
result.refresh()
```

```{code-cell} ipython3
result.metadata[uuid]["domain"]
```

## Multiple backups

```{code-cell} ipython3
more_metadata = {"time_period": "1m"}
result.update_metadata(uuid=uuid, to_update=more_metadata)
```

```{code-cell} ipython3
result.view_backup(uuid=uuid, version=2)
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

+++



+++



```{code-cell} ipython3

```
