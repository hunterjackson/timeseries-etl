import jsonschema
import json
import pkg_resources
import yaml
from timeseries_etl.utils import field_type_check

# loading in all of the kafka schema here to avoid constantly reloading it
resource_package = __name__
kafka_msg_schema_stream = pkg_resources.resource_stream(resource_package, '/'.join(('schemas', 'kafka-message.schema.json')))
kafka_msg_schema = json.load(kafka_msg_schema_stream)
kafka_msg_schema_stream.close()


def validate_kafka_messsage(msg, schema=kafka_msg_schema):
    """

    :param msg: loaded json message to validate
    :param schema: schema to validate against
    :return: boolean
    """
    jsonschema.validate(msg, schema, format_checker=jsonschema.FormatChecker())

    # validate field types
    for key, val in msg.items():
        if key[0] == '_':
            continue

        field_type_check(val['value'], val['type'])

    return True


def validate_transform_config(msg):
    """

    :param msg: loaded yaml message to validate
    :return: boolean
    """
    rel_path_to_schema = '/'.join(('schemas', 'transform_configuration.schema.yaml'))
    transform_config_stream = pkg_resources.resource_stream(resource_package, rel_path_to_schema)
    transform_config_schema = yaml.load(transform_config_stream)
    transform_config_stream.close()

    jsonschema.validate(msg, transform_config_schema)

    return True
