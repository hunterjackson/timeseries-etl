from kafka import KafkaProducer
import json

topic = 'test'

producer = KafkaProducer(bootstrap_servers='kafka'  # Connect to kafka cluster on host 'kafka'
                         , value_serializer=lambda v: json.dumps(v).encode('utf-8'))  # configure to serialize messages as json

producer.send(topic, {'key': 'value'})  # send message

producer.flush()  # wait for messages to send, does not confirm messaged received
