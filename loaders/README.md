# Loader Functional Requirements

 1. Connect to source as defined per loader
 2. Increment and load all data in time range in a systematic way from a defined Kafka topic formatted as a transport document into the defined source
	 1. Will need to cache current location in data loading process
 3.  Will need to attempt re-connection if disconnected
 4. Will need to receive and interpret Instruction Documents from Kafka indicating a time period in which to re-load data

