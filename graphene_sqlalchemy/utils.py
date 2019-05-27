import graphene
from sqlalchemy.exc import ArgumentError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import class_mapper, object_mapper
from sqlalchemy.orm.exc import UnmappedClassError, UnmappedInstanceError


def get_session(context):
    return context.get("session")


def get_query(model, context):
    query = getattr(model, "query", None)
    if not query:
        session = get_session(context)
        if not session:
            raise Exception(
                "A query in the model Base or a session in the schema is required for querying.\n"
                "Read more http://docs.graphene-python.org/projects/sqlalchemy/en/latest/tips/#querying")
        query = session.query(model)
        session.close()
    return query


def _symbol_name(column_name, is_asc):
    return column_name + ("_asc" if is_asc else "_desc")


class EnumValue(str):
    """Subclass of str that stores a string and an arbitrary value in
    the "value" property"""

    def __new__(cls, str_value, value):
        return super(EnumValue, cls).__new__(cls, str_value)

    def __init__(self, str_value, value):
        super(EnumValue, self).__init__()
        self.value = value


# Cache for the generated enums, to avoid name clash
_ENUM_CACHE = {}


def _sort_enum_for_model(cls, name=None, symbol_name=_symbol_name):
    name = name or cls.__name__ + "SortEnum"
    if name in _ENUM_CACHE:
        return _ENUM_CACHE[name]
    items = []
    default = []
    for column in inspect(cls).columns.values():
        asc_name = symbol_name(column.name, True)
        asc_value = EnumValue(asc_name, column.asc())
        desc_name = symbol_name(column.name, False)
        desc_value = EnumValue(desc_name, column.desc())
        if column.primary_key:
            default.append(asc_value)
        items.extend(((asc_name, asc_value), (desc_name, desc_value)))
    enum = graphene.Enum(name, items)
    _ENUM_CACHE[name] = (enum, default)
    return enum, default


def sort_argument_for_model(cls, has_default=True):
    """Returns a Graphene argument for the sort field that accepts a list of
    sorting directions for a model.
    If `has_default` is True (the default) it will sort the result by
    the primary key(s)
    """
    enum, default = _sort_enum_for_model(cls)
    if not has_default:
        default = None
    return graphene.Argument(graphene.List(enum), default_value=default)


def get_column_doc(column):
    return getattr(column, "doc", None)


def is_column_nullable(column):
    return bool(getattr(column, "nullable", True))


def is_column_has_default(column):
    return bool(getattr(column, "default", None) is not None)


def is_column_required(column, for_input=False):
    if for_input:
        return not (is_column_has_default(column)
                    or is_column_nullable(column))
    return not is_column_nullable(column)


def is_mapped_class(cls):
    try:
        class_mapper(cls)
    except (ArgumentError, UnmappedClassError) as _:
        print(_)
        return False
    else:
        return True


def is_mapped_instance(cls):
    try:
        object_mapper(cls)
    except (ArgumentError, UnmappedInstanceError) as _:
        print(_)
        return False
    else:
        return True
