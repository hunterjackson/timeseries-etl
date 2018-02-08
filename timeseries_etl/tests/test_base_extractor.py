import unittest
import yaml
import os
from timeseries_etl.extractors.base.base import BaseExtractor


class BasicStandup(unittest.TestCase):

    def setUp(self):
        self.file_location = '/tmp/extractor_test.yaml'
        self.configurations = {'to_topic': 'raw',
                          'from_source': {'name': 'local'},
                          'filter': {'a0': 0}}
        with open(self.file_location, 'w') as f:
            f.write(yaml.dump(self.configurations))

        self.test_instantiation()

    def test_instantiation(self):
        try:
            class BaseTestClass(BaseExtractor):
                def parse(self):
                    pass
                def ingest(self):
                    pass

            self.obj = BaseTestClass(self.file_location)
        except Exception as e:
            self.fail(e)

    def test_configs_loading(self):
        self.assertEqual(self.configurations, self.obj.configurations)

        self.obj.configurations = self.configurations
        self.assertEqual(self.configurations, self.obj.configurations)

    def test_config_type_assignment(self):
        for t in [6, 6.0]:
            with self.assertRaises(TypeError):
                self.obj.configurations = t

    def bad_filename(self):
        with self.assertRaises(FileNotFoundError):
            self.obj.configurations = 'bad_filename'


    def tearDown(self):
        os.remove(self.file_location)


class AbstractTests(unittest.TestCase):

    def setUp(self):
        self.file_location = '/tmp/extractor_test.yaml'
        self.configurations = {'to_topic': 'raw',
                          'from_source': {'name': 'local'},
                          'filter': {'a0': 0}}
        with open(self.file_location, 'w') as f:
            f.write(yaml.dump(self.configurations))

    def test_abstract_functions(self):

        class Inherited(BaseExtractor):
            def ingest(self):
                pass

        with self.assertRaises(TypeError):
            Inherited(self.file_location)

        class Inherited(BaseExtractor):
            def parse(self):
                pass

        with self.assertRaises(TypeError):
            Inherited(self.file_location)


if __name__ == '__main__':
    unittest.main()
