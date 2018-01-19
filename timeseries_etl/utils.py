import builtins
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
