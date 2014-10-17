Map-side join: FOR SMALL-BIG JOINs

sparkjoin_map.scala is a map-side join program using only RDD manipulations, with fields reached ordinally. Use custom function to realize left-join.

sparkjoinsql_map.scala uses predefined schema for table fields. Still use custom left-join function.

Usage:
  
  submit the programs separetely as standalone applications.

  Use ../sparkjoin.sbt to build sparkjoin_map.scala

  Use ../sparkjoinsql.sbt to build sparkjoinsql_map.scala


