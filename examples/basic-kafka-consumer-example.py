from kafka import KafkaConsumer
from kafka import TopicPartition

topic = 'test'
hostname = 'kafka'

consumer = KafkaConsumer(# topic,  # only used in subscription mode
                         bootstrap_servers='kafka'  # hostname
                         # , group_id='python-test-group'  # consumer group, not going to use this method though
                         , auto_offset_reset='earliest')  # tells the consumer to start from the queue if an offset is not present

# assign to all partitions of topic, now assigned instead of subscribed
consumer.assign([TopicPartition(topic, partition) for partition in consumer.partitions_for_topic(topic)])

for msg in consumer:
    print(msg)
