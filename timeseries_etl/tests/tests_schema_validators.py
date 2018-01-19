import unittest
import pkg_resources
import json
from copy import deepcopy
from timeseries_etl.schema_validators import validate_kafka_messsage
from timeseries_etl.schema_validators import kafka_msg_schema
from jsonschema.exceptions import ValidationError


class ValidateKafkaMessageTest(unittest.TestCase):

    base_message = {'timestamp': '2017-01-01T00:00:00Z',
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

    def test_kafka_message_timestamp(self):
        """
        Exercise the different methods of inserting a timestamp
        :return:
        """
        msg = deepcopy(self.base_message)
        self.assertTrue(validate_kafka_messsage(msg), msg='Basic valid transport message returning as invalid')

        msg['timestamp'] = 'junk'
        with self.assertRaises(ValidationError):
            validate_kafka_messsage(msg)

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

if __name__ == '__main__':
    unittest.main()

