import graphene
import sys
from sqlalchemy.inspection import inspect


class MutationOptions(graphene.types.mutation.MutationOptions):
    session = None
    user_roles = None
    roles_map = None

    def freeze(self):
        if 'pytest' in sys.modules:
            return
        return super().freeze()


class Mutation(graphene.Mutation):
    @classmethod
    def __init_subclass_with_meta__(cls,
                                    resolver=None,
                                    output=None,
                                    session=None,
                                    user_roles=None,
                                    roles_map=None,
                                    arguments=None,
                                    _meta=None,
                                    **options):
        if not _meta:
            _meta = MutationOptions(cls)
        _meta.user_roles = user_roles
        _meta.roles_map = roles_map
        _meta.session = session
        assert _meta.session, 'db session not provided'
        super().__init_subclass_with_meta__(
            resolver, output, arguments, _meta, **options)

    @classmethod
    def mutate(cls, root, info, input=None):
        db_session = cls._meta.session
        if callable(db_session):
            db_session = db_session(info)
        assert db_session, 'db session not provided'

        user_roles = cls._meta.user_roles
        if callable(user_roles):
            user_roles = user_roles(info)

        roles_map = cls._meta.roles_map
        if callable(roles_map):
            roles_map = roles_map(info)

        data = input.to_dictionary(db_session)
        output = cls._meta.output
        assert output, f'no output for {cls}'
        new_record = cls.upsert(
            output._meta.model, db_session, user_roles, roles_map, **data)
        return output(**new_record.as_dict())

    @classmethod
    def upsert(cls, model_cls, session, user_roles, roles_map, **data):
        data_for_update = cls._available_fields_for_user(
            user_roles, roles_map, **data)

        model_pk = data.get(inspect(model_cls).primary_key[0].name)
        model = session.query(model_cls).get(model_pk) if model_pk else None

        try:
            if not model:
                model = model_cls(**data_for_update)
                session.add(model)
            else:
                for field, value in data_for_update.items():
                    if getattr(model, field) == value:
                        continue
                    setattr(model, field, value)
            session.commit()
        except Exception as e:
            session.rollback()
            session.close()
            raise e

        return model

    @classmethod
    def _get_fields_for_role(cls, role, roles_map, **data):
        allowed_fields = {}
        disallowed_fields = {}
        fields_for_role = roles_map.get(role) if roles_map else '*'

        if isinstance(fields_for_role, list):
            fields_for_role.append('id')
            for k, v in data.items():
                if k not in fields_for_role:
                    disallowed_fields[k] = v
                else:
                    allowed_fields[k] = v
        elif fields_for_role == '*':
            allowed_fields.update(data)

        return allowed_fields, disallowed_fields

    @classmethod
    def _available_fields_for_user(cls, user_roles, roles_map, **data):
        _fields = {}
        _allowed_fields = {}
        _disallowed_fields = {}

        if not roles_map:
            _fields.update(cls._get_fields_for_role('*', roles_map, **data))
        else:
            available_roles = list(set(roles_map.keys()) & set(user_roles))

            if not available_roles:
                raise Exception('No roles for user')

            for role in available_roles:
                allowed_fields, disallowed_fields = cls._get_fields_for_role(
                    role, roles_map, **data
                )
                _allowed_fields.update(allowed_fields)
                _disallowed_fields.update(disallowed_fields)

            allowed_field_names = list(
                set(_allowed_fields.keys()) & set(data.keys())
            )
            disallowed_fields_names = list(
                set(_disallowed_fields.keys()) - set(allowed_field_names)
            )

            if disallowed_fields_names:
                for n in disallowed_fields_names:
                    if data.get(n) is not None:
                        raise Exception(f'Field {n} not allowed for user')

            _fields.update(_allowed_fields)

        return _fields
