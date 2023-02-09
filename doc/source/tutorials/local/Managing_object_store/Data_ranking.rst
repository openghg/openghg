------------
Ranking data
------------

Sometimes within the object store there will be multiple pieces of data which fulfill a similiar criteria but where one is preferred over the other in certain situations. The most common example of this is with measurements taken at sites where there multiple inlets at different heights. Having all data available is useful but it is often the case that the user will want to use the most representative data for a time period. Ranking is a way for users to rank data sets within a time period to apply that expert preference within an object store and to simplify the workflow for users of the data.

To set the ranking for complementary datasets a few pieces of information will need to be provided:
 1. The associated keys and values for the data sources you want to add the rule for e.g. site, inlet, species values
 2. The associated key for the data sources which will be *different* between the datasets e.g. the keyword "inlet" (the value of this must be included in 1.)
 3. The date range this is relevant to

For more than 1 ranking input, the order that these pieces of information are stored will determine what is returned when the ranking is applied. This will initially be determined by the order the ranks are added, with the most recent being the most important, **but this can be modified as needed** (REQUIREMENT - CHECK WE CAN DO THIS).

E.g. for data from Tacolneston which spans, in this example, from 2009-2012, and all inlets were available for the whole time period::

 add_data_rank(match={"site": "tac", "inlet": "100m"}, different="inlet")  # OMITTING start and end dates - should cover whole range
 add_data_rank(match={"site": "tac", "species": "co2", "inlet": "10m"}, different="inlet", start_date="2012-01-01", end_date="2013-01-01")  # Or could omit end_date here..?

For two datasets, for co2 this should be interpreted as::

 Dataset 1 - 100m; Dataset 2 - 10m 
 
 ALL DATA          |--------------------------------------------|
 Rule A - 100m (1) |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
 Rule B - 10m  (2) |                               xxxxxxxxxxxxx|
 Resolved          |11111111111111111111111111111112222222222222|


For other gases this should be interpreted as::

 ALL DATA          |--------------------------------------------|
 Rule A - 100m (1) |xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx|
 Resolved          |11111111111111111111111111111111111111111111|


Viewing current ranks
---------------------

To see all the current rank rules you can use the function::

 view_data_ranks()  # This will show all rank information
 
This will return a pandas DataFrame with the rank details as stored.

You can also refine your view by passing keywords::

 view_data_ranks(site="tac")

To view associated rules / ranks for a set of datasets as a plot the `plot_rank_stack` plotting function can be used. For example to see the rules associated with Tacolneston co2 data::

 plot_rank_stack(site="tac", species="co2")

If an unambiguous stack which cannot be determined you will be asked to provide more details e.g. ::

 plot_rank_stack(site="tac")

is not refined enough as there are different rules for different gases meaning this can't be visualised simply as a stack.

> **QUESTION:**

**HOW WOULD WE DETERMINE AN UNAMBIGUOUS STACK VERSUS AMBIGUOS - WHAT DOES THAT MEAN?** - Could include "different" column in database which includes the key for the value which is different e.g. just the keyword "inlet". Maybe ambiguous could be "can't have anything else different than the 'different' column"?

> **QUESTION:** Would something like::
 
 plot_rank_stack(keywords={site:"tac", species:"co2"}) 

be better? For consistency with add_data_rank? Or should we update add_data_rank?


Updating / deleting (/reordering) ranks
---------------------------------------

> **QUESTION: Should these be created to use (`match` or keywords) and `different`??**

e.g.::

 delete_data_rank(match={site:"tac", species:"co2", inlet:"10m"}, different="inlet")  # ???

 update_data_rank(...)  # More complex - what needs updating? How does this need to be updated?

> **QUESTION:Could reordered be included as part of update or it's own thing? Or too difficult? Maybe rather than reordering suggest, deleting and readding? Would need to be easy to do...**

> **QUESTION: Back up and restore?**


Using the ranks
---------------

*This is the real question...*

::

 get_obs_surface(..., ranking=True)  # By default

::

 search_results.retrieve(..., ranking=False) # By default

- If single source can't be resolved from the ranking - return an error
 
(Ranking could be applied in the layer between search and retrieve).


Code design
-----------

Overall requirements
 - Order of ranks determines importance
 - Ability to re-order the ranking?
 - Ability to delete ranks from the database

Functions:
 - add_data_rank - add new rank to database
 - view_data_ranks - view current ranks in database
 - plot_rank_stack - plot ranks as a stack (selected ranks need to be plottable as a stack)
 - delete_data_rank - delete rank from database
 - update_data_rank - update rank in database
 - UPDATE: retrieve method of search results?
 - UPDATE: get_... functions (should link to search results but need to add additional input for ranking=True)

add_data_rank function (or add_data_rule, add_rank function?)
 - Inputs:

   - keys and values for data (doesn't have to map to exactly one data source?)
   - different keys (how does that work for inlet=53m etc...)
   - start_date and end_date (i.e. date range) BUT if they're not passed that's fine we should be able to infer from the data?

 - Can we use these inputs to grab all relevant data in the database? 

> **QUESTION:** How do we grab the rules from the database to build up the stack? Seems like this could be a filtering and pandas question...?

plot_rank_stack function
 - Do we want to allow largely keywords arguments here?

   - And/or just the same dictionary form as used for add_data_rank?

 - Needs to produce a sensible error message if an "unambiguous" stack cannot be determined - just needs to find a useful way to ask for more keywords?
 - Show rules in rows, alongside overall determined information to be passed back.

get_obs_surface(..., ranking=True)  # By default

search_results.retrieve(..., ranking=False) # By default

Ranking could be applied in the layer between search and retrieve.
 - Need to think about how this is applied...
 - What could be returned from the `combine_rank` type function and how can this be applied?

    - Within Experiment_rank_opinions.py this was returned as a list of dictionaries containing continuous sets of dates - would this be better as an object...? Or does not everything need to be an object...?
    - List of `Rank` type objects? DataClass with start, end, keys (and different?) for example

e.g.::
    
    expected_output = [{"start":Timestamp("2011-01-01"), "end":Timestamp("2012-01-01"), "keys":None},
                       {"start":Timestamp("2012-01-01"), "end":Timestamp("2012-06-01"), "keys":{"inlet":"10m"}},
                       {"start":Timestamp("2012-06-01"), "end":Timestamp("2012-09-01"), "keys":{"inlet":"50m"}},
                       {"start":Timestamp("2012-09-01"), "end":Timestamp("2012-12-01"), "keys":{"inlet":"100m"}},
                       {"start":Timestamp("2012-12-01"), "end":Timestamp("2013-01-01"), "keys":{"inlet":"50m"}},
                       {"start":Timestamp("2013-01-01"), "end":Timestamp("2014-01-01"), "keys":None}]

This would need to be mapped against the data ranges for the collected data sources (returned from search) - will likely need to update what the "keys" information here but this was just a placeholder. Note: that None meant there was no ranking information for that date range (but that's ok as long as only 1 data source covers that time period).

*(Need to be careful with start and end date inclusion but should be fine if we're consistent...)*


Database
--------

> **QUESTION:** Do we want to add the rank associated with each piece of information or with a central data base? I think central database seems like it would make more sense e.g. csv / other form of database? How can we update this database? Using pandas or something else?

Extra columns added as needed depending on the ranks added? - always need "different", "start_date" and "end_date"
 - Would need to check if column already existed and if not add a column
 - Also need to check that the metadata key exists at all? Could we do this by checking against the metastore in some way?

Stored csv (or otherwise)::

 site,species,inlet,different,start_date,end_date
 tac,,100m,inlet,,
 tac,co2,10m,inlet,2012-01-01,2013-01-01

--> DataFrame::

 site	species	inlet	different	start_date	end_date
 tac 	NaN    	100m 	inlet    	NaN       	NaN    
 tac 	co2  	10m  	inlet    	2012-01-01	2013-01-01



