import unittest
import pkg_resources
import json
from copy import deepcopy
from timeseries_etl.schema_validators import validate_kafka_messsage
from timeseries_etl.schema_validators import kafka_msg_schema
from timeseries_etl.schema_validators import validate_transform_config
from timeseries_etl.schema_validators import validate_loader_config
from timeseries_etl.schema_validators import validate_extractor_config


from jsonschema.exceptions import ValidationError


class ValidateKafkaMessageTest(unittest.TestCase):

    base_message = {'timestamp': {'value': '2017-01-01T00:00:00Z', 'type': 'datetime'},
                    '_document_type': 'transport',
                    'field1': {'value': 1, 'type': 'int'},
                    'field2': {'value': 5.8, 'type': 'float'},
                    'field3': {'value': 'test', 'type': 'str'},
                    'field4': {'value': '2017-01-01T00:00:00Z', 'type': 'datetime'},
                    'field5': {'value': True, 'type': 'bool'}
                    }

    def test_correct_schema_imported(self):
        """
        Ensure correct schema is being imported
        :return:
        """
        resource_package = __name__
        schema_stream = pkg_resources.resource_stream(resource_package, '/'.join(('..', 'schemas', 'kafka-message.schema.json')))
        schema = json.load(schema_stream)
        schema_stream.close()
        self.assertEqual(kafka_msg_schema, schema)

    def test_kafka_message_doc_type(self):
        """
        Exercise different values in _document_type
        :return:
        """
        msg = deepcopy(self.base_message)
        self.assertTrue(validate_kafka_messsage(msg))

        msg['_document_type'] = 'instruction'
        self.assertTrue(validate_kafka_messsage(msg))

        msg['_document_type'] = 'junk'
        with self.assertRaises(ValidationError):
            validate_kafka_messsage(msg)

    def test_kafka_message_fields(self):

        msg = deepcopy(self.base_message)

        del msg['field1']['value']
        with self.assertRaises(ValidationError, msg='No value in field and still passed'):
            validate_kafka_messsage(msg)

        msg['field1']['value'] = 10
        del msg['field1']['type']
        with self.assertRaises(ValidationError, msg='No type in field and still passed'):
            validate_kafka_messsage(msg)

        msg['field1']['type'] = 'durpidy'
        with self.assertRaises(ValidationError, msg='Invalid type in field and still passed'):
            validate_kafka_messsage(msg)

        del msg['field1'], msg['field2'], msg['field3'], msg['field4'], msg['field5']
        with self.assertRaises(ValidationError, msg='No fields in message and still passed'):
            validate_kafka_messsage(msg)

    def test_kafka_message_field_types(self):

        # int process as str
        msg = deepcopy(self.base_message)
        msg['field1']['type'] = 'str'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)

        # str process at int
        msg = deepcopy(self.base_message)
        msg['field3']['type'] = 'int'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)

        # str process as float
        msg['field3']['type'] = 'float'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)

        # date process as int
        msg['field4']['type'] = 'int'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)

        # date process as str should pass
        msg = deepcopy(self.base_message)
        msg['field4']['type'] = 'str'
        self.assertTrue(validate_kafka_messsage(msg))

        # bool pass as str
        msg = deepcopy(self.base_message)
        msg['field5']['type'] = 'str'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)

        # invalid datetime string as datetime
        msg = deepcopy(self.base_message)
        msg['timestamp']['value'] = 'definitely not a timestamp'
        with self.assertRaises(TypeError):
            validate_kafka_messsage(msg)


class ValidateTransformConfigTest(unittest.TestCase):
    base_message = {'from_topic': 'raw',
                    'to_topic': 'transformed',
                    'filter': {'state': {'value': 'UP'}},
                    'grouper': {'function': 'bytime', 'setting1': 'yes'},
                    'manipulation': [{'pivot': {'axis': [1, 2]}}, {'dropna': []}],
                    'aggregation': {'column_1': 'last', 'column_2': 'avg'}}

    def test_validate_transform_config(self):
        self.assertTrue(validate_transform_config(self.base_message))


class ValidateLoaderConfigTest(unittest.TestCase):
    base_message = {'from_topic': 'transformed',
                    'to_source': {'name': 'local', 'directory': '/tmp/test'},
                    'filter': {'state': {'value': 'UP'}}}

    def test_validate_transform_config(self):
        self.assertTrue(validate_loader_config(self.base_message))


class ValidateExtractorConfigTest(unittest.TestCase):
    base_message = {'from_topic': 'transformed',
                    'from_source': {'name': 'local', 'directory': '/tmp/test'},
                    'filter': {'state': {'value': 'UP'}}}

    def test_validate_transform_config(self):
        self.assertTrue(validate_extractor_config(self.base_message))


if __name__ == '__main__':
    unittest.main()
