from sqlalchemy import inspect


class ModelHistoryProxy(object):
    """ Proxy object to gain access to historical model attributes.

    This leverages SqlAlchemy attribute history to provide access to the previous value of an attribute.
    """

    def __init__(self, instance):
        self.__instance = instance
        self.__inspect = inspect(instance)

    def __getattr__(self, key):
        # Get the attr
        attr = getattr(self.__inspect.attrs, key)

        # Examine attribute history
        # If a value was deleted (e.g. replaced) -- we return it as the previous version.
        # Otherwise it's unchanged, and we return the current value
        history = attr.history
        if not history.deleted:
            # No previous value, return the current value instead
            return attr.value
        else:
            # Return the previous value
            # It's a tuple, since History supports collections, but we do not support these,
            # so just get the first element
            return history.deleted[0]
