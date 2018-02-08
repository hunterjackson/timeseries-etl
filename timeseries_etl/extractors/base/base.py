import abc
import yaml
from kafka import KafkaProducer
from timeseries_etl.utils import obj_to_json
from timeseries_etl.schema_validators import validate_extractor_config


class BaseExtractor(abc.ABC):

    def __init__(self, yaml_configuration_file):
        """
        The Base of all extraction classes

        :param yaml_configuration_file: location of file with yaml configurations
        """
        self._connected = False
        self._configurations = None

        self.kafka_connection = None
        self.configurations = yaml_configuration_file

    @abc.abstractmethod
    def ingest(self):
        pass

    @abc.abstractmethod
    def parse(self):
        pass

    def connect(self):
        self._connected = True

    def kafka_connect(self, hostname):
        self.kafka_connection = KafkaProducer(bootstrap_servers=hostname,
                                              value_serializer=obj_to_json)

    @property
    def configurations(self):
        return self._configurations

    @configurations.setter
    def configurations(self, configs):
        """
        Configurations setter function
        :param configs: File location with yaml configs or a dictionary contain preloaded configs
        :return:
        """
        if type(configs) is str:
            with open(configs) as f:
                configs = yaml.load(f)
                validate_extractor_config(configs)
                self._configurations = configs

        elif type(configs) is dict:
            validate_extractor_config(configs)
            self._configurations = configs

        else:
            raise TypeError('configs must either be string location of a yaml file or a dictionary of configs')

