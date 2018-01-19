import builtins
import json
from dateutil.parser import parse as date_parser


def field_type_check(value, value_type: str):
    """
    Made to test the string representation of a type against a value
    :param value:
    :param value_type:
    :return:
    """
    try:

        if value_type == 'datetime':
            date_parser(value)
        else:
            assert type(value) is getattr(builtins, value_type)

    except (AssertionError, ValueError, TypeError):
        raise TypeError

    return True


def read_transport_message(msg: str, validate_schema=True):

    msg = json.loads(msg)
    if msg['_document_type'] != 'transport':
        raise TypeError('Not a transport document')

    if validate_schema:
        # importing here to avoid import loop
        from timeseries_etl.schema_validators import validate_kafka_messsage
        validate_kafka_messsage(msg)

    # convert datetime fields
    for k, v in msg.items():
        if k[0] == '_':
            continue
        if v['type'] == 'datetime':
            msg[k]['value'] = date_parser(v['value'], ignoretz=True)

    return msg
