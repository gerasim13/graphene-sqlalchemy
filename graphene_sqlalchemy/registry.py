class Registry(object):
    def __init__(self):
        self._registry_models = {}
        self._registry_composites = {}
        self._registry_attributes = {}
        self._registry_enums = {}

    def __contains__(self, item):
        return item in self._registry_models or \
               item in self._registry_composites or \
               item in self._registry_attributes

    def register(self, cls):
        from .types import ObjectType
        assert issubclass(cls, ObjectType),\
            f'Only classes of type {ObjectType} can be registered, ' \
            f'received "{cls.__name__}"'
        assert cls._meta.registry == self,\
            'Registry for a Model have to match.'
        self._registry_models[cls._meta.model] = cls

    def get_type_for_model(self, model):
        return self._registry_models.get(model)

    def register_attributes(self, attributes_cls):
        self._registry_attributes[attributes_cls.__name__] = attributes_cls

    def get_attributes_for_model(self, attributes):
        return self._registry_attributes.get(attributes)

    def register_composite_converter(self, composite, converter):
        self._registry_composites[composite] = converter

    def get_converter_for_composite(self, composite):
        return self._registry_composites.get(composite)

    def register_enum(self, enum):
        self._registry_enums[enum.__name__] = enum

    def get_enum(self, enum):
        return self._registry_enums.get(enum)


registry = None


def get_global_registry():
    global registry
    if not registry:
        registry = Registry()
    return registry


def reset_global_registry():
    global registry
    registry = None
