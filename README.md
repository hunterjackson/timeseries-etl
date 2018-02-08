[![Build Status](https://travis-ci.org/hunterjackson/timeseries_etl.svg?branch=master)](https://travis-ci.org/hunterjackson/timeseries_etl)
# timeseries-etl
## An Extensible Time Series ETL
Made for POC purposes

## High Level Goals

 1. Minimal Mandatory Configurations (Just Works)
 2. Easily Extensible at Every Level
 3. Scalable

## High Level Diagram
![High level Architecture Diagram](https://i.imgur.com/Iu6OXTY.jpg)

## Data Storage

There are two supported ways to store data. Either in a database of some kind or in a file store containing Parquet files with a specific naming scheme. 

### File Name Schema
\<ISODatetime of starttime>_\_\<ISODatetime of endtime\>.avro

Example:
20170101T00:01:11.132Z__20170101T00:01:54.043Z.avro

### Data Format
Regardless of the storage device the device will be stored as shown in the example below

|timestamp| arbitrary_column_name_1 | arbitrary_column_name_2 ...| 
|--|--|--|
| ISODatetime | value2 | value3 |
.
.
.

Example:
|timestamp| length | width | 
|--|--|--|
| 20170101T00:01:11.132 | 4 | 3 |
| 20170101T00:01:11.133 | 5 | 1 |
| 20170101T00:01:11.134 | 4 | 3 |
| 20170101T00:01:11.135 | 5 | 2 |


You get the idea right? O also this shouldn't have to be said but the timestamp is in UTC
