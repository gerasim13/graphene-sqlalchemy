import enum
import functools
import graphene
import sqlalchemy
import sqlalchemy_utils
from collections import OrderedDict
from graphene.types.utils import yank_fields_from_attrs
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import interfaces
from sqlalchemy.orm.base import object_mapper
from sqlalchemy.sql import type_api

from .fields import default_connection_field_factory
from .registry import Registry, get_global_registry
from .utils import (get_column_doc, is_column_required, is_column_nullable,
                    is_mapped_class, get_model_primary_key)


class FieldType(enum.Enum):
    scalar = enum.auto()
    composite = enum.auto()
    hybrid = enum.auto()
    relationship = enum.auto()


def iter_fields(model, only_fields=(), exclude_fields=()):
    mapper = inspect(model, raiseerr=False) or model

    def _skip_field_with_name(name):
        is_not_in_only = only_fields and name not in only_fields
        is_excluded = name in exclude_fields
        # We skip this field if we specify only_fields and is not
        # in there. Or when we exclude this field in exclude_fields\
        return is_not_in_only or is_excluded

    synonyms = getattr(mapper, 'synonyms', [])
    columns = getattr(mapper, 'columns', [])
    composites = getattr(mapper, 'composites', [])
    all_orm_descriptors = getattr(mapper, 'all_orm_descriptors', [])
    relationships = getattr(mapper, 'relationships', [])

    for f in relationships:
        if _skip_field_with_name(f.key):
            continue
        yield f.key, f, FieldType.relationship

    for name, f in composites:
        if _skip_field_with_name(f.name):
            continue
        yield f.name, f, FieldType.composite

    for f in columns:
        if _skip_field_with_name(f.name):
            continue
        yield f.name, f, FieldType.scalar

    for f in synonyms:
        field = None
        if isinstance(f.name, sqlalchemy.Column):
            field = f.name
        elif isinstance(f.name, str):
            field = getattr(columns, f.name)
        if field is None or _skip_field_with_name(field.name):
            continue
        yield f.class_attribute.key, field, FieldType.scalar

    for f in all_orm_descriptors:
        if type(f) != hybrid_property or _skip_field_with_name(f.__name__):
            continue
        yield f.__name__, f, FieldType.hybrid


def construct_fields(
        model, registry,
        only_fields=(),
        exclude_fields=(),
        field_types=(),
        type_cast=None,
        connection_field_factory=None,
        input_attributes=False):
    fields = OrderedDict()
    type_cast = type_cast or dict()
    relationship_keys = list()
    conv_functions = {
        FieldType.scalar: convert_sqlalchemy_field,
        FieldType.composite: convert_sqlalchemy_composite,
        FieldType.hybrid: convert_sqlalchemy_hybrid_method,
        FieldType.relationship: convert_sqlalchemy_relationship,
    }

    for name, field, type in iter_fields(model, only_fields, exclude_fields):
        if type not in field_types:
            continue

        if type is FieldType.relationship:
            foreign_key_field = next(
                iter(field._user_defined_foreign_keys), None)
            foreign_key_name = name
            if foreign_key_field is not None:
                foreign_key_name = foreign_key_field.name
            relationship_keys.append(foreign_key_name)
        # Skip foreign fields for one-to-many relationship
        elif type is FieldType.scalar and name in relationship_keys:
            continue

        if name in type_cast:
            cast_type = type_cast.get(name)
            converted_field = cast_type(
                description=get_column_doc(field),
                required=is_column_required(field, input_attributes))
        else:
            conv_fn = conv_functions.get(type)
            conv_field_factory = connection_field_factory or default_connection_field_factory
            converted_field = conv_fn(
                type,
                field,
                name,
                registry=registry,
                connection_field_factory=conv_field_factory,
                input_attributes=input_attributes)

        if not converted_field:
            continue
        fields[name] = converted_field
    return fields


def get_attributes_fields(
        models,
        registry=None,
        field_types=(),
        only_fields=(),
        exclude_fields=(),
        type_cast=None,
        connection_field_factory=None,
        input_attributes=False):
    _fields = yank_fields_from_attrs(construct_fields(
        models,
        registry,
        field_types=field_types,
        only_fields=only_fields,
        exclude_fields=exclude_fields,
        type_cast=type_cast,
        connection_field_factory=connection_field_factory,
        input_attributes=input_attributes
    ), _as=graphene.Field)
    return _fields


def convert_model_to_attributes(m,
                                registry=None,
                                connection_field_factory=None,
                                attributes_name=None,
                                input_attributes=False,
                                type_cast=None,
                                only_fields=(),
                                exclude_fields=()):
    if not registry:
        registry = get_global_registry()
    assert isinstance(registry, Registry), f'The attribute registry in ' \
        f'{registry.__class__.__name__} needs to be an instance of Registry, '\
        f'received {registry}.'

    if not attributes_name:
        attributes_name = m.__name__ + 'Attribute'
    attributes = registry.get_attributes_for_model(attributes_name)

    if not attributes:
        _fields = get_attributes_fields(
            m, registry,
            field_types=(FieldType.scalar, FieldType.relationship),
            only_fields=only_fields,
            exclude_fields=exclude_fields,
            type_cast=type_cast,
            connection_field_factory=connection_field_factory,
            input_attributes=input_attributes)
        attributes = type(attributes_name, (object,), _fields)
        registry.register_attributes(attributes)
    return attributes


def convert_sqlalchemy_composite(composite, name,
                                 registry=None,
                                 connection_field_factory=None,
                                 input_attributes=False):
    from .types import InputObjectType, ObjectType
    if input_attributes:
        _classname = f'{composite.name}Input'
        _baseclass = InputObjectType
    else:
        _classname = composite.name
        _baseclass = ObjectType

    graphene_type = registry.get_converter_for_composite(_classname)
    if not graphene_type:
        _fields = get_attributes_fields(
            composite, registry,
            field_types=(FieldType.scalar, FieldType.relationship),
            connection_field_factory=connection_field_factory,
            input_attributes=input_attributes)
        graphene_type = type(_classname, (_baseclass,), _fields)
        registry.register_composite_converter(_classname, graphene_type)

    return graphene_type


def convert_sqlalchemy_model_to_scheme(model, name, classname, baseclass,
                                       registry=None,
                                       connection_field_factory=None,
                                       input_attributes=False):
    graphene_type = registry.get_type_for_relationship_input(classname)
    if graphene_type:
        return graphene_type

    primary_key = get_model_primary_key(model)
    _fields = {
        'Meta': {'model': model},
        primary_key.name: convert_sqlalchemy_type(
            primary_key.type,
            primary_key,
            primary_key.name,
            registry,
            connection_field_factory,
            input_attributes)
    }

    if not input_attributes:
        exclude = (primary_key.name,)
        for _name, _field, _type in iter_fields(model, exclude_fields=exclude):
            if isinstance(_field, hybrid_property):
                _fields[_name] = convert_sqlalchemy_hybrid_method(
                    FieldType.hybrid,
                    _field,
                    _name,
                    registry,
                    connection_field_factory,
                    input_attributes)
            else:
                _fields[_name] = convert_sqlalchemy_type(
                    _field.type,
                    _field,
                    _name,
                    registry,
                    connection_field_factory,
                    input_attributes)

    graphene_type = type(classname, (baseclass,), _fields)
    registry.register_type_for_relationship_input(graphene_type)
    return graphene_type


def convert_sqlalchemy_relationship(t, relationship, name,
                                    registry=None,
                                    connection_field_factory=None,
                                    input_attributes=False):
    from .types import InputObjectType
    direction = relationship.direction
    model = relationship.mapper.entity

    if not input_attributes:
        def dynamic_type():
            _type = registry.get_type_for_model(model)
            if not _type:
                return None
            if direction == interfaces.MANYTOONE or not relationship.uselist:
                return graphene.Field(_type)
            elif direction in (interfaces.ONETOMANY, interfaces.MANYTOMANY):
                if _type._meta.connection:
                    return connection_field_factory(relationship, registry)
                return graphene.Field(graphene.List(_type))
        return graphene.Dynamic(dynamic_type)

    if direction == interfaces.MANYTOONE or not relationship.uselist:
        foreign_key_field = next(iter(
            relationship._user_defined_foreign_keys), None)
        graphene_type = convert_sqlalchemy_field(
            FieldType.scalar,
            foreign_key_field,
            name,
            registry,
            connection_field_factory,
            input_attributes,
            optional_field=True)
        return graphene_type

    graphene_type = convert_sqlalchemy_model_to_scheme(
        model,
        name,
        f'{model.__name__}RelationshipInput',
        InputObjectType,
        registry,
        connection_field_factory,
        input_attributes)
    return graphene.List(
        graphene_type,
        description=get_column_doc(relationship),
        required=is_column_required(relationship, input_attributes))


def convert_sqlalchemy_hybrid_method(t, f, name,
                                     registry=None,
                                     connection_field_factory=None,
                                     input_attributes=False):
    property_type = f.info.get('type')
    is_abstract = False
    if hasattr(property_type, '__abstract__'):
        is_abstract = property_type.__abstract__
    if is_mapped_class(property_type) or is_abstract:
        _classname = property_type.__name__
        _baseclass = graphene.ObjectType
        if input_attributes:
            _classname += 'Input'
            _baseclass = InputObjectType
        graphene_type = convert_sqlalchemy_model_to_scheme(
            property_type,
            name or f.__name__,
            _classname,
            _baseclass,
            registry,
            connection_field_factory,
            input_attributes)
        return graphene.Field(graphene_type)
    elif hasattr(property_type, '_to_instance'):
        property_type = property_type._to_instance(property_type)
        return convert_sqlalchemy_type(
            property_type, f, name,
            registry,
            connection_field_factory,
            input_attributes)
    return graphene.String(
        name=name or f.__name__,
        description=getattr(f, "__doc__", None),
        required=False)


def convert_sqlalchemy_field(t, f, name,
                             registry=None,
                             connection_field_factory=None,
                             input_attributes=False,
                             optional_field=False):
    return convert_sqlalchemy_type(f.type, f, name, registry,
                                   connection_field_factory,
                                   input_attributes,
                                   optional_field)


def convert_id_field(t, f, name,
                     registry=None,
                     connection_field_factory=None,
                     input_attributes=False,
                     optional_field=False):
    is_required = is_column_required(f, input_attributes)
    assert input_attributes or is_required, f.name
    return graphene.ID(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@functools.singledispatch
def convert_sqlalchemy_type(t, f, name,
                            registry=None,
                            connection_field_factory=None,
                            input_attributes=False,
                            optional_field=False):
    raise Exception(f"Don't know how to convert the sqlalchemy type {t} "
                    f"({t.__class__})")


@convert_sqlalchemy_type.register(postgresql.UUID)
def convert_uuid_field(t, f, name,
                       registry=None,
                       connection_field_factory=None,
                       input_attributes=False,
                       optional_field=False):
    if getattr(f, 'primary_key', None):
        return convert_id_field(
            t, f, name,
            registry,
            connection_field_factory,
            input_attributes,
            optional_field)
    if not f.type.as_uuid:
        return convert_str_field(
            t, f, name,
            registry,
            connection_field_factory,
            input_attributes,
            optional_field)
    is_required = is_column_required(f, input_attributes)
    return graphene.UUID(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.String)
@convert_sqlalchemy_type.register(sqlalchemy.Text)
@convert_sqlalchemy_type.register(sqlalchemy.Unicode)
@convert_sqlalchemy_type.register(sqlalchemy.UnicodeText)
@convert_sqlalchemy_type.register(sqlalchemy.VARCHAR)
@convert_sqlalchemy_type.register(sqlalchemy.NVARCHAR)
@convert_sqlalchemy_type.register(sqlalchemy.TEXT)
@convert_sqlalchemy_type.register(sqlalchemy.ForeignKey)
@convert_sqlalchemy_type.register(sqlalchemy_utils.ColorType)
@convert_sqlalchemy_type.register(sqlalchemy_utils.CountryType)
def convert_str_field(t, f, name,
                      registry=None,
                      connection_field_factory=None,
                      input_attributes=False,
                      optional_field=False):
    if getattr(f, 'primary_key', None):
        return convert_id_field(
            t, f, name,
            registry,
            connection_field_factory,
            input_attributes,
            optional_field)
    is_required = is_column_required(f, input_attributes)
    return graphene.String(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.SmallInteger)
@convert_sqlalchemy_type.register(sqlalchemy.Integer)
@convert_sqlalchemy_type.register(sqlalchemy.INTEGER)
def convert_int_field(t, f, name,
                      registry=None,
                      connection_field_factory=None,
                      input_attributes=False,
                      optional_field=False):
    if getattr(f, 'primary_key', None):
        return convert_id_field(
            t, f, None,
            registry,
            connection_field_factory,
            input_attributes,
            optional_field)
    is_required = is_column_required(f, input_attributes)
    return graphene.Int(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Float)
@convert_sqlalchemy_type.register(sqlalchemy.FLOAT)
@convert_sqlalchemy_type.register(sqlalchemy.DECIMAL)
def convert_float_field(t, f, name,
                        registry=None,
                        connection_field_factory=None,
                        input_attributes=False,
                        optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.Float(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Boolean)
@convert_sqlalchemy_type.register(sqlalchemy.BOOLEAN)
def convert_bool_field(t, f, name,
                       registry=None,
                       connection_field_factory=None,
                       input_attributes=False,
                       optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.Boolean(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.DateTime)
@convert_sqlalchemy_type.register(sqlalchemy.DATETIME)
def convert_datetime_field(t, f, name,
                           registry=None,
                           connection_field_factory=None,
                           input_attributes=False,
                           optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.DateTime(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Date)
@convert_sqlalchemy_type.register(sqlalchemy.DATE)
def convert_date_field(t, f, name,
                       registry=None,
                       connection_field_factory=None,
                       input_attributes=False,
                       optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.Date(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Time)
@convert_sqlalchemy_type.register(sqlalchemy.TIME)
@convert_sqlalchemy_type.register(sqlalchemy.TIMESTAMP)
def convert_datetime_field(t, f, name,
                           registry=None,
                           connection_field_factory=None,
                           input_attributes=False,
                           optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.Time(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.JSON)
@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSONB)
def convert_json_field(t, f, name,
                       registry=None,
                       connection_field_factory=None,
                       input_attributes=False,
                       optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.JSONString(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Enum)
def convert_enum_field(t, f, name,
                       registry=None,
                       connection_field_factory=None,
                       input_attributes=False,
                       optional_field=False):
    enum_class = getattr(t, 'enum_class', None)
    graphene_type = registry.get_enum(enum_class.__name__)

    if not graphene_type:
        if enum_class:
            # Check if an enum.Enum type is used
            graphene_type = graphene.Enum.from_enum(enum_class)
        else:
            # Nope, just a list of string options
            items = zip(t.enums, t.enums)
            graphene_type = graphene.Enum(t.name, items)
        registry.register_enum(graphene_type)

    is_required = is_column_required(f, input_attributes)
    return graphene.Field(
        graphene_type,
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy_utils.ChoiceType)
def convert_choice_type_field(t, f, name,
                              registry=None,
                              connection_field_factory=None,
                              input_attributes=False,
                              optional_field=False):
    choices = t.choices
    graphene_type = registry.get_enum(choices.__name__)

    if not graphene_type:
        if isinstance(choices, enum.EnumMeta):
            # Check if an enum.Enum type is used
            graphene_type = graphene.Enum.from_enum(choices)
        else:
            # Nope, just a list of string options
            graphene_type = graphene.Enum(choices.__name__, choices)
        registry.register_enum(graphene_type)

    is_required = is_column_required(f, input_attributes)
    return graphene.Field(
        graphene_type,
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.ARRAY)
def convert_array_field(t, f, name,
                        registry=None,
                        connection_field_factory=None,
                        input_attributes=False,
                        optional_field=False):
    field = convert_sqlalchemy_type(t.item_type, f,
                                    name,
                                    registry,
                                    connection_field_factory,
                                    input_attributes)
    is_required = is_column_required(f, input_attributes)
    return graphene.List(
        field.type if hasattr(field, 'type') else type(field),
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy.Table)
def convert_table(t, f, name,
                  registry=None,
                  connection_field_factory=None,
                  input_attributes=False,
                  optional_field=False):
    is_required = is_column_required(f, input_attributes)
    return graphene.Dynamic(
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)


@convert_sqlalchemy_type.register(sqlalchemy_utils.CompositeType)
def convert_composite_type_field(t, f, name,
                                 registry=None,
                                 connection_field_factory=None,
                                 input_attributes=False,
                                 optional_field=False):
    graphene_type = convert_sqlalchemy_composite(t, registry,
                                                 connection_field_factory,
                                                 input_attributes)
    is_required = is_column_required(f, input_attributes)
    return graphene.Field(
        graphene_type,
        name=name or f.name,
        description=get_column_doc(f),
        required=not optional_field and is_required)
