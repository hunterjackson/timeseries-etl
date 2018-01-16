# Transform Functional Requirements

 Leveraging pyspark connect to fetch and process data from a kafka queue
## Groupers
1. Function for defining how time records are to be grouped
2. Will be easily exchanged and defined

Leveraging pyspark a manipulation function will perform actions on the dataframe itself unlike the aggregation functions that perform only on a column.
## Manipulation Functions
1. Defined functions can be performed on a dataframe or a subset of the dataframe.
2. will be easily exchanged, extended, and stacked.

Leveraging pyspark a function will be performed on each column of data in the group so that a single row per group emerges
## Aggregation Functions
1. Defined functions that can be performed on a column of a pyspark dataframe to compress the defined group of rows down to a single row.
2. Will be easily exchanged and defined

