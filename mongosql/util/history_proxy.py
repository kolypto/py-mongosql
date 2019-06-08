from copy import deepcopy
from sqlalchemy import inspect
from sqlalchemy.orm.base import DEFAULT_STATE_ATTR
from sqlalchemy.orm.state import InstanceState

from mongosql.bag import ModelPropertyBags


class ModelHistoryProxy:
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
        # Save the information that we'll definitely need
        self.__instance = instance
        self.__model = self.__instance.__class__
        self.__bags = ModelPropertyBags.for_model(self.__model)  # type: ModelPropertyBags
        self.__inspect = inspect(instance)  # type: InstanceState

        # Copy every field onto ourselves
        self.__copy_from_instance(self.__instance)

        # Enable accessing relationships through our proxy
        self.__install_instance_state(instance)

    def __copy_from_instance(self, instance):
        """ Copy all attributes of `instance` to `self`

        Alright, this code renders the whole point of having ModelHistoryProxy void.
        There is an issue with model history:

            "Each time the Session is flushed, the history of each attribute is reset to empty.
             The Session by default autoflushes each time a Query is invoked"
             https://docs.sqlalchemy.org/en/latest/orm/internals.html#sqlalchemy.orm.state.AttributeState.history

        This means that as soon as you load a relationship, model history is reset.
        To solve this, we have to make a copy of this model.
        All attributes are set on `self`, so accessing `self.attr` will not trigger `__getattr__()`
        """
        """ Copy the given list of columns from the instance onto self """
        insp = self.__inspect  # type: InstanceState

        # Copy all values onto `self`
        for column_name in self.__bags.columns.names:
            # Skip unloaded columns (because that would emit sql queries)
            # Also skip the columns that were already copied (perhaps, mutable columns?)
            if column_name not in insp.unloaded:
                # The state
                attr_state = insp.attrs[column_name]  # type: AttributeState

                # Get the historical value
                # deepcopy() ensures JSON and ARRAY values are copied in full
                hist_val = deepcopy(_get_historical_value(attr_state))

                # Remove the value onto `self`: we're bearing the value now
                setattr(self, column_name, hist_val)

    def __install_instance_state(self, instance):
        """ Install an InstanceState, so that relationship descriptors can work properly """
        # These lines install the internal SqlAlchemy's property on our proxy
        # This property mimics the original object.
        # This ensures that we can access relationship attributes through a ModelHistoryProxy object
        # Example:
        # hist = ModelHistoryProxy(comment)
        # hist.user.id  # wow!
        instance_state = getattr(instance, DEFAULT_STATE_ATTR)
        my_state = InstanceState(self, instance_state.manager)
        my_state.key = instance_state.key
        my_state.session_id = instance_state.session_id
        setattr(self, DEFAULT_STATE_ATTR, my_state)

    def __getattr__(self, key):
        # Get a relationship:
        if key in self.__bags.relations:
            relationship = getattr(self.__model, key)
            return relationship.__get__(self, self.__model)

        # Get a property (@property)
        if key in self.__bags.properties:
            # Because properties may use other columns,
            # we have to run it against our`self`, because only then it'll be able to get the original values.
            return getattr(self.__model, key).fget(self)

        # Every column attribute is accessed through history
        attr = self.__inspect.attrs[key]
        return _get_historical_value(attr)


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
