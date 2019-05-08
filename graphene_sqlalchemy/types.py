import graphene
from sqlalchemy.orm.exc import NoResultFound

from .converter import (convert_model_to_attributes, get_attributes_fields,
                        FieldType)
from .fields import default_connection_field_factory
from .registry import get_global_registry, Registry
from .utils import (is_mapped_class, is_mapped_instance, get_query,
                    input_to_dictionary)


class ObjectTypeOptions(graphene.types.objecttype.ObjectTypeOptions):
    model = None
    registry = None
    connection = None
    connection_field_factory = None
    attributes = None
    id = None


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
        connection=None,
        connection_class=None,
        use_connection=None,
        interfaces=(),
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
                model, connection_field_factory=connection_field_factory)

        if use_connection is None and interfaces:
            use_connection = any(
                (issubclass(interface, graphene.relay.Node) for interface in interfaces)
            )

        if use_connection and not connection:
            # We create the connection automatically
            if not connection_class:
                connection_class = graphene.relay.Connection
            connection = connection_class.create_type(
                f'{cls.__name__}Connection', node=cls
            )

        if connection is not None:
            assert issubclass(connection, graphene.relay.Connection),\
                f'The connection must be a Connection.' \
                f'Received {connection.__name__}'

        if not _meta:
            _meta = ObjectTypeOptions(cls)

        _meta.id = id or "id"
        _meta.attributes = attributes
        _meta.connection = connection
        _meta.connection_field_factory = connection_field_factory
        _meta.registry = registry
        _meta.model = model

        _fields = {
            n: getattr(attributes, n) for n in dir(attributes)
            if (not callable(getattr(attributes, n)) and
                not n.startswith('__'))
        }
        _fields.update(get_attributes_fields(
            model,
            registry,
            field_types=(
                FieldType.composite, FieldType.hybrid, FieldType.relationship
            )
        ))

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
        model = cls._meta.model
        return get_query(model, info.context)

    @classmethod
    def get_node(cls, info, id):
        try:
            return cls.get_query(info).get(id)
        except NoResultFound:
            return None

    @classmethod
    def resolve_id(cls, root, info, **args):
        if hasattr(root, '__mapper__'):
            keys = root.__mapper__.primary_key_from_instance(root)
            return tuple(keys) if len(keys) > 1 else keys[0]
        return root.id


class InputObjectType(graphene.InputObjectType):
    @classmethod
    def __init_subclass_with_meta__(cls, container=None, _meta=None, **options):
        def _iter_fields(attributes):
            for i in vars(attributes):
                f = getattr(attributes, i)
                if not isinstance(f, graphene.Field):
                    continue
                yield i, f

        schema = options.pop('schema', None)
        if schema:
            attributes = convert_model_to_attributes(
                schema._meta.model,
                connection_field_factory=schema._meta.connection_field_factory,
                input_attributes=True)
            for name, field in _iter_fields(attributes):
                setattr(cls, name, field)

        super().__init_subclass_with_meta__(
            container=container,
            _meta=_meta,
            **options)


class MutationOptions(graphene.types.mutation.MutationOptions):
    session_getter = None


class Mutation(graphene.Mutation):

    @classmethod
    def __init_subclass_with_meta__(cls,
                                    resolver=None,
                                    output=None,
                                    session_getter=None,
                                    arguments=None,
                                    _meta=None,
                                    **options):
        if not _meta:
            _meta = MutationOptions(cls)
        _meta.session_getter = session_getter or cls.get_session
        super().__init_subclass_with_meta__(
            resolver, output, arguments, _meta, **options)

    @classmethod
    def get_session(cls, info):
        return None

    @classmethod
    def mutate(cls, root, info, input=None):
        data = input_to_dictionary(input)
        output = cls._meta.output
        assert output, f'no output for {cls}'

        try:
            db_session = cls._meta.session_getter(info)
            if db_session:
                new_record = output._meta.model(**data)
                db_session.add(new_record)
                db_session.commit()
        except Exception as e:
            db_session.rollback()
            raise e
        return output(**new_record.as_dict())
