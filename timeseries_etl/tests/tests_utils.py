import unittest
from datetime import datetime
from timeseries_etl.utils import field_type_check


class FieldTypeCheckTest(unittest.TestCase):

    # valid use cases
    def test_valid_type_checks(self):
        self.assertTrue(field_type_check(1, 'int'))
        self.assertTrue(field_type_check(1.0, 'float'))
        self.assertTrue(field_type_check('test', 'str'))
        self.assertTrue(field_type_check('2017-01-01T00:00:00Z', 'datetime'))

    def test_non_valid_int(self):
        with self.assertRaises(TypeError):
            field_type_check(1.0, 'int')

        with self.assertRaises(TypeError):
            field_type_check('balck', 'int')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'int')

    def test_non_valid_float(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'float')

        with self.assertRaises(TypeError):
            field_type_check('balck', 'float')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'float')

    def test_non_valid_str(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'str')

        with self.assertRaises(TypeError):
            field_type_check(1.1, 'str')

        with self.assertRaises(TypeError):
            field_type_check(datetime.now(), 'str')

    def test_non_valid_datetime(self):
        with self.assertRaises(TypeError):
            field_type_check(1, 'datetime')

        with self.assertRaises(TypeError):
            field_type_check(1.1, 'datetime')

        with self.assertRaises(TypeError):
            field_type_check('definitely not a date', 'datetime')