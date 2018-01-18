from dateutil.parser import parse as date_parser


def field_type_check(value, value_type: str):
    try:

        if value_type == 'int':
            assert type(value) is int
        elif value_type == 'float':
            assert type(value) is float
        elif value_type == 'str':
            assert type(value) is str
        elif value_type == 'datetime':
            date_parser(value)
        else:
            raise ValueError('Unknown type {}'.format(value_type))
    except (AssertionError, ValueError):
        raise ValueError
