import unittest
import json
from datetime import datetime
from timeseries_etl.utils import read_transport_message
from timeseries_etl.utils import field_type_check
from timeseries_etl.utils import to_transport_message
from jsonschema.exceptions import ValidationError
from copy import deepcopy


class FieldTypeCheckTest(unittest.TestCase):

    # valid use cases
    def test_valid_type_checks(self):
        self.assertTrue(field_type_check(1, 'int'))
        self.assertTrue(field_type_check(1.0, 'float'))
        self.assertTrue(field_type_check('test', 'str'))
        self.assertTrue(field_type_check('2017-01-01T00:00:00Z', 'datetime'))
        self.assertTrue(field_type_check(True, 'bool'))

    def test_non_valid_int(self):
        with self.assertRaises(TypeError):
            field_type_check(1.0, 'int')

        with self.assertRaises(TypeError):
            field_type_check('balck', 'int')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'int')

        with self.assertRaises(TypeError):
            field_type_check(True, 'int')

    def test_non_valid_float(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'float')

        with self.assertRaises(TypeError):
            field_type_check('balck', 'float')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'float')

        with self.assertRaises(TypeError):
            field_type_check(True, 'float')

    def test_non_valid_str(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'str')

        with self.assertRaises(TypeError):
            field_type_check(1.1, 'str')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'str')

        with self.assertRaises(TypeError):
            field_type_check(True, 'str')

    def test_non_valid_datetime(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'datetime')

        with self.assertRaises(TypeError):
            field_type_check(1.1, 'datetime')

        with self.assertRaises(TypeError):
            field_type_check('definitely not a date', 'datetime')

        with self.assertRaises(TypeError):
            field_type_check(True, 'datetime')

    def test_non_valid_bool(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'bool')

        with self.assertRaises(TypeError):
            field_type_check(1.1, 'bool')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'bool')

        with self.assertRaises(TypeError):
            field_type_check('1', 'bool')


class ReadTransportMessageTest(unittest.TestCase):
    base_message = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                    "_document_type": "transport",
                    "field1": {"value": 1, "type": "int"},
                    "field2": {"value": 5.8, "type": "float"},
                    "field3": {"value": "test", "type": "str"},
                    "field4": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                    "field5": {"value": true, "type": "bool"}
                    }'''

    base_read_message = {"timestamp": {"value": datetime(2017, 1, 1, 0, 0, 0), "type": "datetime"},
                    "_document_type": "transport",
                    "field1": {"value": 1, "type": "int"},
                    "field2": {"value": 5.8, "type": "float"},
                    "field3": {"value": "test", "type": "str"},
                    "field4": {"value": datetime(2017, 1, 1, 0, 0, 0), "type": "datetime"},
                    "field5": {"value": True, "type": "bool"}
                    }

    def test_type_return(self):
        self.assertIs(dict, type(read_transport_message(self.base_message)))

    def test_basic_message(self):
        # test timestamp and int
        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                    "_document_type": "transport",
                    "field1": {"value": 1, "type": "int"}}'''
        msg_out = {k: v for k, v in self.base_read_message.items() if k in ('_document_type', 'timestamp', 'field1')}

        self.assertEqual(read_transport_message(msg_in, validate_schema=False), msg_out)

        # test float
        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                            "_document_type": "transport",
                            "field2": {"value": 5.8, "type": "float"}}'''
        msg_out = {k: v for k, v in self.base_read_message.items() if k in ('_document_type', 'timestamp', 'field2')}

        self.assertEqual(read_transport_message(msg_in, validate_schema=False), msg_out)

        # test string
        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                                    "_document_type": "transport",
                                    "field3": {"value": "test", "type": "str"}}'''
        msg_out = {k: v for k, v in self.base_read_message.items() if k in ('_document_type', 'timestamp', 'field3')}

        self.assertEqual(read_transport_message(msg_in, validate_schema=False), msg_out)

        # test bool

        # test string
        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                                    "_document_type": "transport",
                                    "field5": {"value": true, "type": "bool"}}'''
        msg_out = {k: v for k, v in self.base_read_message.items() if k in ('_document_type', 'timestamp', 'field5')}

        self.assertEqual(read_transport_message(msg_in, validate_schema=False), msg_out)

    def test_schema_validation(self):
        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                                    "_document_type": "transport",
                                    "field5": {"value": true}}'''
        with self.assertRaises(ValidationError, msg='Test for Schema Validation'):
            read_transport_message(msg_in, validate_schema=True)

        msg_in = '''{"timestamp": {"value": "2017-01-01T00:00:00Z", "type": "datetime"},
                                            "_document_type": "transport",
                                            "field5": {"value": true, "type": "int"}}'''
        with self.assertRaises(TypeError, msg='Test for type validation'):
            read_transport_message(msg_in, validate_schema=True)


class ToTransportMessageTest(unittest.TestCase):
    base_message = {"timestamp": {"value": datetime(2017, 1, 1, 0, 0, 0), "type": "datetime"},
                    "_document_type": "transport",
                    "field1": {"value": 1, "type": "int"},
                    "field2": {"value": 5.8, "type": "float"},
                    "field3": {"value": "test", "type": "str"},
                    "field4": {"value": datetime(2017, 1, 1, 0, 0, 0), "type": "datetime"},
                    "field5": {"value": True, "type": "bool"}}

    def test_type_check(self):
        msg_in = deepcopy(self.base_message)
        self.assertIs(str, type(to_transport_message(msg_in, validate_schema=False)))

    def test_datetime_transform(self):
        msg_in = deepcopy(self.base_message)
        msg_out = json.loads(to_transport_message(msg_in, validate_schema=False))
        self.assertEqual(self.base_message['timestamp']['value'].isoformat(), msg_out['timestamp']['value'])

    def test_schema_validation(self):

        with self.assertRaises(ValidationError):
            msg_in = deepcopy(self.base_message)
            del msg_in['timestamp']
            to_transport_message(msg_in, validate_schema=True)

        with self.assertRaises(TypeError):
            msg_in = deepcopy(self.base_message)
            msg_in['timestamp']['type'] = 'int'
            to_transport_message(msg_in, validate_schema=True)


if __name__ == '__main__':
    unittest.main()