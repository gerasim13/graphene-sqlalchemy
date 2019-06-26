import graphene
from sqlalchemy.inspection import inspect


class MutationOptions(graphene.types.mutation.MutationOptions):
    session = None
    user_roles = None
    roles_map = None


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

    @staticmethod
    def _available_fields_for_user(user_roles, roles_map, **data):
        available_roles = list(set(roles_map.keys()) & set(user_roles))
        if not available_roles:
            raise Exception('No roles for user')

        fields = {}
        for role in available_roles:
            fields_for_role = roles_map.get(role)
            if isinstance(fields_for_role, list):
                fields_for_role.append('id')
                for k, v in data.items():
                    if v and k not in fields_for_role:
                        raise Exception(f'Field {k} not allowed for user')
                    fields[k] = v
            elif fields_for_role == '*':
                fields.update(data)

        return fields
