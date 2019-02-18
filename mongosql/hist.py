from __future__ import absolute_import

from sqlalchemy import inspect
from sqlalchemy.orm.util import object_state
import copy

from sqlalchemy.orm.state import InstanceState


class ModelHistoryProxy(object):
    """ Proxy object to gain access to historical model attributes.

    This leverages SqlAlchemy attribute history to provide access to the previous value of an
    attribute. The only reason why this object exists is because keeping two instances in memory may
    be expensive. But because normally you'll only need a field or two, the decision was to use
    this magic proxy object that will load model history on demand.

    Why would you need to access model history at all?
    Because CrudHelper's update method (i.e., changing model fields) gives you two objects: the
    current instance, and the old instance, so that your custom code in the update handler can
    compare those fields.
    For instance, when a certain object is being moved from one User to another, you might want
    to notify both of them. In that case, you'll need access to the historical user.

    The initial solution was to *copy* the instance, apply the modifications from JSON to a copy,
    and then feed both of them to the save handler... but copying was expensive.
    That's why we have this proxy: it does not load all the fields of the historical model,
    but acts as a proxy object (__getattr__()) that will get those properties on demand.
    """

    def __init__(self, instance):
        # First, save the information that we'll definitely need
        self.__instance = instance  # the object
        self.__inspect = inspect(instance)  # its inspection info
        self.__relations = frozenset(self.__inspect.mapper.relationships.keys())  # relationship names

        self.__copy_instance_to(instance)
        # TODO: Introduce two modes to MongoSQL: explicit eager-loading (only those excplicitly
        #  specified, with raiseload() or noload() on everything else),
        #  or an "i-agree-that-it-will-be-slow", in which case this object also takes the slow
        #  and painful path.

    def __copy_instance_to(self, instance):
        """ Copy all attributes of `instance` to `self`

        Alright, this code renders the whole point of having ModelHistory void.
        There is an issue with model history:
        "Each time the Session is flushed, the history of each attribute is reset to empty.
         The Session by default autoflushes each time a Query is invoked"
        This means that as soon as you load a relationship, model history is reset.
        To solve this, we have to make a copy of this model.
        All attributes are set on `self`, so accessing `self.attr` will not trigger `__getattr__()`
        """
        # Copy all values onto `self`
        for key, val in self.__inspect.attrs.items():
            if key not in self.__relations:  # skip relationships
                # Get the historical value
                # Deep copy will copy JSON values as well
                hist_val = copy.deepcopy(_get_historical_value(val))
                # Remove the value onto `self`: we're the proxy now
                setattr(self, key, hist_val)

        # These lines install the internal SqlAlchemy's property on our proxy
        # This property mimics the original object.
        # This ensures that we can access relationship attributes through a ModelHistoryProxy object
        # Example:
        # hist = ModelHistoryProxy(comment)
        # hist.user.id  # wow!
        self._sa_instance_state = InstanceState(self, instance._sa_instance_state.manager)
        self._sa_instance_state.key = instance._sa_instance_state.key
        self._sa_instance_state.session_id = instance._sa_instance_state.session_id

    def __getattr__(self, key):
        # This method only handles those elements that were not already handled by __init__()

        # Get a relationship:
        if key in self.__relations:
            ent_class = self.__instance.__class__
            prop = getattr(ent_class, key)
            return prop.__get__(self, ent_class)
        # Get a property (@property)
        if isinstance(getattr(self.__instance.__class__, key, None), property):
            return getattr(self.__instance.__class__, key).fget(self)
        # Get a value from the instance itself
        return getattr(self.__instance, key)


def _get_historical_value(attr):
    """ Get the previous value of an attribute

        This is where the magic happens: this method goes into the SqlAlchemy instance and
        obtains the historical value of an attribute called `key`
    """
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
