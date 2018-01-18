import jsonschema
import json
import pkg_resources

resource_package = __name__
kafka_msg_schema_stream = pkg_resources.resource_stream(resource_package, '/'.join(('..', 'schemas', 'kafka-message.schema.json')))
kafka_msg_schema = json.load(kafka_msg_schema_stream)


def validate_kafka_messsage(msg, schema=kafka_msg_schema):
    """

    :param msg: json message to validate
    :param schema: schema to validate against
    :return:
    """
    jsonschema.validate(msg, schema)

    return True
