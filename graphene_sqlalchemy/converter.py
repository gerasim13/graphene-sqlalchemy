import enum
import functools
from collections import OrderedDict

import graphene
import sqlalchemy
from graphene.types.utils import yank_fields_from_attrs
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import interfaces

from .fields import default_connection_field_factory
from .registry import Registry, get_global_registry
from .utils import get_column_doc, is_column_nullable, is_column_has_default


class FieldType(enum.Enum):
    scalar = enum.auto()
    composite = enum.auto()
    hybrid = enum.auto()
    relationship = enum.auto()


def iter_fields(model, only_fields, exclude_fields):
    mapper = inspect(model)

    def _skip_field_with_name(name):
        is_not_in_only = only_fields and name not in only_fields
        is_excluded = name in exclude_fields
        # We skip this field if we specify only_fields and is not
        # in there. Or when we exclude this field in exclude_fields\
        return is_not_in_only or is_excluded

    for f in mapper.columns:
        if _skip_field_with_name(f.name):
            continue
        yield f.name, f, FieldType.scalar

    for name, f in mapper.composites:
        if _skip_field_with_name(f.name):
            continue
        yield f.name, f, FieldType.composite

    for f in mapper.all_orm_descriptors:
        if type(f) != hybrid_property or _skip_field_with_name(f.__name__):
            continue
        yield f.__name__, f, FieldType.hybrid

    for f in mapper.relationships:
        if _skip_field_with_name(f.key):
            continue
        yield f.key, f, FieldType.relationship


def construct_fields(
        model, registry,
        only_fields=(),
        exclude_fields=(),
        field_types=(),
        connection_field_factory=None,
        input_attributes=False):
    fields = OrderedDict()
    conv_functions = {
        FieldType.scalar: convert_sqlalchemy_field,
        FieldType.composite: convert_sqlalchemy_composite,
        FieldType.hybrid: convert_sqlalchemy_hybrid_method,
        FieldType.relationship: convert_sqlalchemy_relationship,
    }
    for name, field, type in iter_fields(model, only_fields, exclude_fields):
        if type not in field_types:
            continue
        conv_fn = conv_functions.get(type)
        converted_field = conv_fn(
            field,
            registry,
            connection_field_factory or default_connection_field_factory,
            input_attributes)
        if not converted_field:
            continue
        fields[name] = converted_field
    return fields


def get_attributes_fields(
        models,
        registry=None,
        field_types=(),
        connection_field_factory=None,
        input_attributes=False):
    _fields = yank_fields_from_attrs(construct_fields(
        models,
        registry,
        field_types=field_types,
        connection_field_factory=connection_field_factory,
        input_attributes=input_attributes
    ), _as=graphene.Field)
    return _fields


def convert_sqlalchemy_field(f, registry,
                             connection_field_factory=None,
                             input_attributes=False):
    return convert_sqlalchemy_type(f.type, f, registry,
                                   connection_field_factory,
                                   input_attributes)


def convert_model_to_attributes(m, f=None, registry=None,
                                connection_field_factory=None,
                                input_attributes=False):
    if not registry:
        registry = get_global_registry()
    assert isinstance(registry, Registry), f'The attribute registry in ' \
        f'{registry.__class__.__name__} needs to be an instance of Registry, '\
        f'received {registry}.'

    if input_attributes:
        _cls_name = m.__name__ + 'InputAttribute'
    else:
        _cls_name = m.__name__ + 'Attribute'

    attributes = registry.get_attributes_for_model(_cls_name)
    if not attributes:
        _fields = get_attributes_fields(
            m, registry,
            field_types=(FieldType.scalar,),
            connection_field_factory=connection_field_factory,
            input_attributes=input_attributes)
        attributes = type(_cls_name, (object,), _fields)
        registry.register_attributes(attributes)
    return attributes


def convert_sqlalchemy_composite(composite, registry,
                                 connection_field_factory=None,
                                 input_attributes=False):
    converter = registry.get_converter_for_composite(composite.composite_class)
    if not converter:
        try:
            raise Exception(
                "Don't know how to convert the composite field %s (%s)"
                % (composite, composite.composite_class)
            )
        # handle fields that are not attached to a class yet
        # (don't have a parent)
        except AttributeError:
            raise Exception(
                "Don't know how to convert the composite field %r (%s)"
                % (composite, composite.composite_class)
            )
    return converter(composite, registry)


def convert_sqlalchemy_relationship(relationship, registry,
                                    connection_field_factory=None,
                                    input_attributes=False):
    direction = relationship.direction
    model = relationship.mapper.entity

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


def convert_sqlalchemy_hybrid_method(f, **kwargs):
    return graphene.String(
        description=getattr(f, "__doc__", None),
        required=False)


def convert_id_field(t, f, registry=None,
                     connection_field_factory=None,
                     input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.ID(description=get_column_doc(f),
                       required=required)


@functools.singledispatch
def convert_sqlalchemy_type(t, f, registry=None,
                            connection_field_factory=None,
                            input_attributes=False):
    raise Exception(f"Don't know how to convert the sqlalchemy type {t} "
                    f"({t.__class__})")


@convert_sqlalchemy_type.register(postgresql.UUID)
def convert_uuid_field(t, f, registry=None,
                       connection_field_factory=None,
                       input_attributes=False):
    if f.primary_key:
        return convert_id_field(t, f, registry,
                                connection_field_factory,
                                input_attributes)

    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.UUID(description=get_column_doc(f),
                         required=required)


@convert_sqlalchemy_type.register(sqlalchemy.String)
@convert_sqlalchemy_type.register(sqlalchemy.Text)
@convert_sqlalchemy_type.register(sqlalchemy.Unicode)
@convert_sqlalchemy_type.register(sqlalchemy.UnicodeText)
@convert_sqlalchemy_type.register(sqlalchemy.VARCHAR)
@convert_sqlalchemy_type.register(sqlalchemy.NVARCHAR)
@convert_sqlalchemy_type.register(sqlalchemy.TEXT)
def convert_str_field(t, f, registry=None,
                      connection_field_factory=None,
                      input_attributes=False):
    if f.primary_key:
        return convert_id_field(t, f, registry,
                                connection_field_factory,
                                input_attributes)

    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.String(description=get_column_doc(f),
                           required=required)


@convert_sqlalchemy_type.register(sqlalchemy.SmallInteger)
@convert_sqlalchemy_type.register(sqlalchemy.Integer)
@convert_sqlalchemy_type.register(sqlalchemy.INTEGER)
def convert_int_field(t, f, registry=None,
                      connection_field_factory=None,
                      input_attributes=False):
    if f.primary_key:
        return convert_id_field(t, f, registry,
                                connection_field_factory,
                                input_attributes)

    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Int(description=get_column_doc(f),
                        required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Float)
@convert_sqlalchemy_type.register(sqlalchemy.FLOAT)
@convert_sqlalchemy_type.register(sqlalchemy.DECIMAL)
def convert_float_field(t, f, registry=None,
                        connection_field_factory=None,
                        input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Float(description=get_column_doc(f),
                          required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Boolean)
@convert_sqlalchemy_type.register(sqlalchemy.BOOLEAN)
def convert_bool_field(t, f, registry=None,
                       connection_field_factory=None,
                       input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Boolean(description=get_column_doc(f),
                            required=required)


@convert_sqlalchemy_type.register(sqlalchemy.DateTime)
@convert_sqlalchemy_type.register(sqlalchemy.DATETIME)
def convert_datetime_field(t, f, registry=None,
                           connection_field_factory=None,
                           input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.DateTime(description=get_column_doc(f),
                             required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Date)
@convert_sqlalchemy_type.register(sqlalchemy.DATE)
def convert_date_field(t, f, registry=None,
                       connection_field_factory=None,
                       input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Date(description=get_column_doc(f),
                         required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Time)
@convert_sqlalchemy_type.register(sqlalchemy.TIME)
@convert_sqlalchemy_type.register(sqlalchemy.TIMESTAMP)
def convert_datetime_field(t, f, registry=None,
                           connection_field_factory=None,
                           input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Time(description=get_column_doc(f),
                         required=required)


@convert_sqlalchemy_type.register(sqlalchemy.JSON)
@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSONB)
def convert_json_field(t, f, registry=None,
                       connection_field_factory=None,
                       input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.JSONString(description=get_column_doc(f),
                               required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Enum)
def convert_enum_to_enum(t, f, registry=None,
                         connection_field_factory=None,
                         input_attributes=False):
    enum_class = getattr(t, 'enum_class', None)
    if enum_class:  # Check if an enum.Enum type is used
        graphene_type = graphene.Enum.from_enum(enum_class)
    else:  # Nope, just a list of string options
        items = zip(t.enums, t.enums)
        graphene_type = graphene.Enum(t.name, items)

    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Field(
        graphene_type,
        description=get_column_doc(f),
        required=required,
    )


@convert_sqlalchemy_type.register(sqlalchemy.ARRAY)
def conver_array_field(t, f, registry=None,
                       connection_field_factory=None,
                       input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    field = convert_sqlalchemy_type(t.item_type, f, registry,
                                    connection_field_factory,
                                    input_attributes)
    return graphene.List(type(field),
                         description=get_column_doc(f),
                         required=required)


@convert_sqlalchemy_type.register(sqlalchemy.Table)
def conver_table(t, f, registry=None,
                 connection_field_factory=None,
                 input_attributes=False):
    if input_attributes:
        required = not (is_column_has_default(f) or is_column_nullable(f))
    else:
        required = not is_column_nullable(f)
    return graphene.Dynamic(description=get_column_doc(f),
                            required=required)
