import json
from kafka import KafkaProducer
from string import ascii_lowercase
from string import digits
from random import randint
from sys import maxsize
from datetime import datetime
from timeseries_etl.schema_validators import validate_kafka_messsage


def generate_column_names(cc):
    columns = list()
    for x in range(cc):
        columns.append('{}{}'.format(ascii_lowercase[x % 26], digits[(x // 26) % 10]))

    return tuple(columns)


def generate_random_record(col_names):
    record = {col_name: {'value': randint(-1 * maxsize, maxsize), 'type': 'int'} for col_name in col_names}
    record['timestamp'] = {'value': datetime.utcnow().isoformat(), 'type': 'datetime'}
    record['_document_type'] = 'transport'
    return record


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='generate and insert random information in to a Kafka Topic')
    parser.add_argument('--topic', default='raw', type=str, help='Name of topic to submit too')
    parser.add_argument('--width', default=3, type=int, help='Number of values in in each record')
    parser.add_argument('--count', default=10, type=int, help='Number of records to submit')
    parser.add_argument('--validate', action='store_true', help='validate each record against schema')

    args = parser.parse_args()
    producer = KafkaProducer(bootstrap_servers='kafka', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    topic = args.topic
    column_count = args.width
    row_count = args.count

    column_names = generate_column_names(column_count)  # generate column names predictably
    records = (generate_random_record(column_names) for _ in range(row_count))  # generate records using the column names with random integers as values

    for record in records:
        if args.validate:
            validate_kafka_messsage(record)  # raises error if not a valid message, here mostly for testing
        producer.send(topic, record)

    producer.flush()  # wait for messages to send, does not confirm message received
