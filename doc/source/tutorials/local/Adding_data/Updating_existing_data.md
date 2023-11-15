.. _updating_existing_data:
Updating existing data
======================

OpenGHG categorises data based on the supplied necessary keywords and stores these as the associated metadata. For each data type these keywords will be different but they will always be used to understand how the data is defined.

When adding data to the object store, two checks will be made against currently stored data:

1. Whether data with the same set of distinct keywords
2. Whether the time range for the data being added overlaps with the current time range for that data.

If the data exists but the time range does not overlap, this data will be added, grouped with the previous data and associated with the same keywords.

By default, if data exists and the time range *does* overlap with existing data, the data will not be added and this will produce a `DataOverlapError`.

Updating data
-------------

To add updated data to the object store which does overlap on time with current data, when using the `standarise_*` functions the user can specify what action to perform in this case using the `if_exists` input. This provides 2 options:

1. "new" - only store the newly added data
2. "combine" - combine the new and previous data and prefer the new data in the case where the time range overlaps.

By default, using this keyword will also create a new version of the data. In this way, the previous data will be retained (saved) but the new / combined data will become the details which are accessed by default.

Managing versions
-----------------

If data files are large or there will be many updates needed, it may not be desirable to save the currently stored data and it may be prefered to overwrite this rather than retain this as a version. Whether to retain or overwrite the current data can set using the `save_current` input.

1. "yes" (/"y") - Save the current data and create a new version for the new / combined data
2. "no" (/"n") - Do not save the current data and replace with the new / combined data

Replacing "identical" data
--------------------------

One check OpenGHG will make will be whether or not an exact copy of this file has been added previously. In this case this will not check within the object store explicitly, and the data will not be added. For the rare cases where this may not be the desired behaviour, the `force` flag using `True` or `False`can be used to bypass this check and attempt to add the data to the object store in the usual way. 

Example
-------

Set up keywords

   ...

Add data the first time:

  standardise_surface()

  search(...)

Shows the data stored within the object store (version 1)

Update data, but only store the new data. By default this will create a new version:

  standardise_surface(..., if_exists="new")

  search(...)

Look at the data, now only includes the new data and version has increased by 1 (version 2)

Update data and combine with previous, but do not retain the previous data:

  standardise_surface(..., if_exists="current", save_current=False)

  search(...)

Now contains combined data but the version has not changed.

Replace the same data file

  standardise_surface(..., force=True)

