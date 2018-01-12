# Extractor Functional Requirements

 1. Connect to source as defined per extractor
 2. Increment and load all data in time range in a systematic way into defined Kafka topic formatted as a transport document
	 1. Will need to cache current location in data extraction process
 3.  Will need to attempt re-connection if disconnected
 4. Will need to receive and interpret Instruction Documents from Kafka indicating a time period in which to re-upload data

