Reduce-side join: FOR BIG-BIG JOINs.

Reduce-Side joins are more simple than Map-Side joins since the input datasets need not to be structured. But it is less efficient as both datasets have to go through the MapReduce shuffle phase. the records with the same key are brought together in the reducer.

As reduce-side join uses existing methods for leftjoin, and none of the methods fit in our scope of not filtering out those lines without join Key, reduce-side solutions get results different from map-side joins.
