from __future__ import absolute_import
from builtins import object

from sqlalchemy import inspect
import copy

from sqlalchemy.orm.state import InstanceState


class ModelHistoryProxy(object):
    """ Proxy object to gain access to historical model attributes.

    This leverages SqlAlchemy attribute history to provide access to the previous value of an attribute.
    """

    def __init__(self, instance):
        self.__instance = instance
        self.__inspect = inspect(instance)
        self.inpsp =         self.__inspect
        self.__relations = frozenset(self.__inspect.mapper.relationships.keys())
        manager = instance._sa_instance_state.manager
        self._sa_instance_state = InstanceState(self, manager)
        self.or_state = instance._sa_instance_state
        for key, val in self.__inspect.attrs.items():
            if key not in self.__relations:
                setattr(self, key, copy.deepcopy(self.__attr_val(val)))
        self._sa_instance_state = InstanceState(self, instance._sa_instance_state.manager)
        self._sa_instance_state.key = instance._sa_instance_state.key
        self._sa_instance_state.session_id = instance._sa_instance_state.session_id

    def __getattr__(self, key):
        # Get the attr
        if key in self.__relations:
            ent_class = self.__instance.__class__
            prop = getattr(ent_class, key)
            return prop.__get__(self, ent_class)

        if isinstance(getattr(self.__instance.__class__, key, None), property):
            return getattr(self.__instance.__class__, key).fget(self)

        return getattr(self.__instance, key)

    def __attr_val(self, attr):
        # Examine attribute history
        # If a value was deleted (e.g. replaced) -- we return it as the previous version.
        history = attr.history
        if not history.deleted:
            # No previous value, return the current value instead
            return attr.value
        else:
            # Return the previous value
            # It's a tuple, since History supports collections, but we do not support these,
            # so just get the first element
            return history.deleted[0]
