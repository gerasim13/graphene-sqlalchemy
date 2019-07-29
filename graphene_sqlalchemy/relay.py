import sys
from collections import OrderedDict
from collections.abc import Iterable
from functools import partial
from graphene.relay.node import GlobalID
from graphene.types import ID, List, Field, Interface, ObjectType, Argument
from graphene.types.base import BaseOptions, BaseType
from graphene.types.utils import get_type
from graphql.type.definition import GraphQLList
from graphql_relay import from_global_id, to_global_id
from inspect import isclass

from .converter import (convert_model_to_attributes, get_attributes_fields,
                        FieldType)
from .fields import default_connection_field_factory


class InterfaceOptions(BaseOptions):
    fields = None  # type: Dict[str, Field]
    filter_fields = None  # type: Dict[str, Field]
    model = None

    def freeze(self):
        if 'pytest' in sys.modules:
            return
        return super().freeze()


class NodeField(Field):
    def __init__(
            self,
            node,
            type=False,
            deprecation_reason=None,
            name=None,
            arguments=None,
            **kwargs):
        assert issubclass(node, Node), "NodeField can only operate in Nodes"
        self.node_type = node
        self.field_type = type

        if not arguments:
            arguments = {'id': ID(required=True)}

        # If we don's specify a type, the field type will be the node
        # interface
        field_type = type or node
        if field_type._meta.return_many:
            field_type = List(field_type)

        super().__init__(
            field_type,
            description="The ID of the object",
            **arguments,
        )

    def get_resolver(self, parent_resolver):
        return partial(self.node_type.node_resolver, get_type(self.field_type))


class AbstractNode(Interface):
    class Meta:
        abstract = True

    @classmethod
    def __init_subclass_with_meta__(
            cls,
            _meta=None,
            **options):
        if not _meta:
            _meta = InterfaceOptions(cls)

        if not _meta.fields:
            _meta.fields = OrderedDict(
                id=GlobalID(cls, description="The ID of the object.")
            )

        super().__init_subclass_with_meta__(
            _meta=_meta, **options)


class Node(AbstractNode):
    """An object with an ID"""

    @classmethod
    def __init_subclass_with_meta__(
            cls,
            model=None,
            attributes=None,
            type_cast=None,
            exclude_fields=None,
            filter_fields=None,
            connection_field_factory=default_connection_field_factory,
            **options):
        assert model, 'Model not provided'
        _meta = InterfaceOptions(cls)
        _meta.model = model

        if filter_fields:
            _meta.filter_fields = {}
            _meta.fields = OrderedDict(
                id=GlobalID(
                    cls,
                    description="The ID of the object.",
                    required=False))
            if not attributes:
                attributes = convert_model_to_attributes(
                    model,
                    connection_field_factory=connection_field_factory,
                    attributes_name=model.__name__ + 'RelayAttributes',
                    only_fields=filter_fields.split(','),
                    type_cast=type_cast,
                    exclude_fields=exclude_fields or ())
            for n in dir(attributes):
                if not (not callable(getattr(attributes, n)) and
                        not n.startswith('__')):
                    continue
                _meta.fields[n] = getattr(attributes, n)
                _meta.filter_fields[n] = Argument(
                    getattr(_meta.fields[n], 'type')
                )

        super().__init_subclass_with_meta__(
            _meta=_meta, **options)

    @classmethod
    def Field(cls, *args, **kwargs):  # noqa: N802
        kwargs.update({'arguments': cls._meta.filter_fields})
        return NodeField(cls, *args, **kwargs)

    @classmethod
    def node_resolver(cls, only_type, root, info, **kwargs):
        if 'id' not in kwargs:
            return cls.get_node_from_filter(info, **kwargs)

        return cls.get_node_from_global_id(
            info,
            kwargs['id'],
            only_type=only_type)

    @classmethod
    def get_node_from_filter(cls, info, **filter_fields):
        _type = cls._meta.model.__name__
        graphene_type = info.schema.get_type(_type).graphene_type
        filter_node = getattr(graphene_type, "filter_node", None)
        return_many = isinstance(info.return_type,  GraphQLList)
        return filter_node(info, return_many=return_many, **filter_fields)

    @classmethod
    def get_node_from_global_id(cls, info, global_id, only_type=None):
        try:
            _type, _id = cls.from_global_id(global_id)
            graphene_type = info.schema.get_type(_type).graphene_type
        except Exception:
            return None

        if only_type:
            assert graphene_type == only_type, f'Must receive a ' \
                f'{only_type._meta.name} id.'

        # We make sure the ObjectType implements the "Node" interface
        if cls not in graphene_type._meta.interfaces:
            return None

        get_node = getattr(graphene_type, "get_node", None)
        if not get_node:
            return None
        return get_node(info, _id)

    @classmethod
    def from_global_id(cls, global_id):
        try:
            global_id = from_global_id(global_id)
        except Exception as _:
            pass
        if isinstance(global_id, str):
            global_id = (cls._meta.model.__name__, global_id)
        return global_id

    @classmethod
    def to_global_id(cls, type, id):
        return to_global_id(type, id)
