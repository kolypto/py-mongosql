class Marker:
    """ An object that can transparently wrap a dict key

        Example:

            d = { Marker('key'): value }

            # You can still use the original key:
            d['key']  # -> value
            'key' in d  # -> True

            # At the same time, your marker key will pass an explicit isinstance() check:
            key, value = d.popitem()
            key == 'key'  # -> True
            isinstance(key, Marker)  # -> True

        This enables you to easily define custom markers, and, for instance,
        keep track of where do dictionary keys originate from!
    """

    __slots__ = ('key',)

    @classmethod
    def unwrap(cls, value):
        """ Unwrap the value if it's wrapped with a marker """
        return value.key if isinstance(value, cls) else value

    def __init__(self, key):
        # Store the original key
        self.key = key

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.key))

    # region Marker is a Proxy

    # All these methods have to be implemented in order to mimic the behavior of a dict key

    def __hash__(self):
        # Dict keys rely on hashes
        # We're ought to have the same hash with the underlying value
        return hash(self.key)

    def __eq__(self, other):
        # Marker equality comparison:
        #  key == key | key == Marker.key
        return self.key == (other.key if isinstance(other, Marker) else other)

    def __bool__(self):
        # Marker truth check:
        # `if include:` would always be true otherwise
        return bool(self.key)

    def __instancecheck__(self, instance):
        # isinstance() will react on both the Marker's type and the value's type
        return isinstance(instance, type(self)) or isinstance(instance, type(self.key))

    # endregion
