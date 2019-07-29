import graphene
import sys
from collections import namedtuple
from graphene.relay.node import InterfaceOptions
from sqlalchemy import or_, types
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import cast

from .converter import (convert_model_to_attributes, get_attributes_fields,
                        FieldType)
from .fields import default_connection_field_factory
from .registry import get_global_registry, Registry
from .relay import Node
from .utils import is_mapped_class, is_mapped_instance, get_query


class ObjectTypeOptions(graphene.types.objecttype.ObjectTypeOptions):
    model = None
    registry = None
    connection = None
    connection_field_factory = None
    attributes = None
    return_many = None
    id = None

    def freeze(self):
        if 'pytest' in sys.modules:
            return
        return super().freeze()


class ObjectType(graphene.ObjectType):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model=None,
        attributes=None,
        registry=None,
        skip_registry=False,
        only_fields=(),
        exclude_fields=(),
        type_cast=None,
        connection=None,
        connection_class=None,
        use_connection=None,
        interfaces=(),
        return_many=None,
        id=None,
        connection_field_factory=default_connection_field_factory,
        _meta=None,
        **options
    ):
        assert is_mapped_class(model),\
            f'You need to pass a valid SQLAlchemy ' \
            f'Model in {cls.__name__}.Meta, received "{model}".'

        if not registry:
            registry = get_global_registry()
        assert isinstance(registry, Registry),\
            f'The attribute registry in {cls.__name__} needs to be an ' \
            f'instance of Registry, received "{registry}".'

        if not attributes:
            attributes = convert_model_to_attributes(
                model,
                connection_field_factory=connection_field_factory,
                attributes_name=model.__name__ + 'Attributes',
                only_fields=only_fields,
                type_cast=type_cast,
                exclude_fields=exclude_fields)

        if use_connection is None and interfaces:
            use_connection = any(
                (issubclass(interface, Node)
                 for interface in interfaces))

        if use_connection and not connection:
            # We create the connection automatically
            if not connection_class:
                connection_class = graphene.relay.Connection
            connection = connection_class.create_type(
                f'{cls.__name__}Connection', node=cls)

        if connection is not None:
            assert issubclass(connection, graphene.relay.Connection),\
                f'The connection must be a Connection.' \
                f'Received {connection.__name__}'

        _fields = {
            n: getattr(attributes, n) for n in dir(attributes)
            if (not callable(getattr(attributes, n)) and
                not n.startswith('__'))
        }
        _fields.update(get_attributes_fields(
            model,
            registry,
            only_fields=only_fields,
            exclude_fields=exclude_fields,
            field_types=(
                FieldType.composite,
                FieldType.hybrid,
                FieldType.relationship
            ),
        ))

        if not _meta:
            _meta = ObjectTypeOptions(cls)

        _meta.id = id or "id"
        _meta.return_many = return_many
        _meta.attributes = attributes
        _meta.connection = connection
        _meta.connection_field_factory = connection_field_factory
        _meta.registry = registry
        _meta.model = model

        if _meta.fields:
            _meta.fields.update(_fields)
        else:
            _meta.fields = _fields

        super().__init_subclass_with_meta__(
            _meta=_meta, interfaces=interfaces, **options
        )

        if not skip_registry:
            registry.register(cls)

    @classmethod
    def is_type_of(cls, root, info):
        if isinstance(root, cls):
            return True
        if not is_mapped_instance(root):
            raise Exception(f'Received incompatible instance "{root}".')
        return isinstance(root, cls._meta.model)

    @classmethod
    def get_query(cls, info):
        return get_query(cls._meta.model, info.context)

    @classmethod
    def get_node(cls, info, id):
        try:
            node = cls.get_query(info).get(id)
            return node
        except NoResultFound:
            return None

    @classmethod
    def filter_node(cls, info, return_many=False, **kwargs):
        try:
            filter_args = []
            query = cls.get_query(info)
            for field, value in kwargs.items():
                column = getattr(cls._meta.model, field)
                if isinstance(column.type, types.ARRAY):
                    if isinstance(value, list):
                        filter_args.append(or_(*[
                            column.contains('{' + v + '}') for v in value]))
                    elif isinstance(value, str):
                        filter_args.append(
                            column.contains(value))
                    else:
                        filter_args.append(column == cast(value, column.type))
                else:
                    filter_args.append(column == cast(value, column.type))
            filter_query = query.filter(*filter_args)
            node = filter_query.all() if return_many else filter_query.first()
            return node
        except NoResultFound:
            return None

    @classmethod
    def resolve_id(cls, root, info, **args):
        if hasattr(root, '__mapper__'):
            keys = root.__mapper__.primary_key_from_instance(root)
            return tuple(keys) if len(keys) > 1 else keys[0]
        return root.id
