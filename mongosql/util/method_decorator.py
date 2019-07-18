from typing import Iterable
from functools import partial, lru_cache, update_wrapper


class method_decorator_meta(type):
    def __instancecheck__(self, method):
        """ Metaclass magic enables isinstance() checks even for decorators wrapped with other decorators """
        # Recursion stopper
        if method is None:
            return False
        # Check the type: isinstance() on self, or on the wrapped object
        # Have to implement isinstance() manually.
        # We use issubclass() to enable it detecting a generic method_decorator: isinstance(something, method_decorator)
        return issubclass(type(method), self) \
               or isinstance(getattr(method, '__wrapped__', None), self)


class method_decorator(metaclass=method_decorator_meta):
    """ A decorator that marks a method, receives arguments, adds metadata, and provides custom group behavior

        Sometimes in Python there's a need to mark some methods of a class and then use them for some sort
        of special processing.

        The important goals here are:
        1) to be able to mark methods,
        2) to be able to execute them transparently,
        3) to be able to collect them and get their names,
        4) to be able to store metadata on them (by receiving arguments)

        I've found out that a good solution would be to implement a class decorator,
        which is also a descriptor. This way, we'll have an attribute that lets you transparently use the method,
        but also knows some metadata about it.

        This wrapper can also contain some business-logic related to this decorator,
        which lets us keep all the relevant logic in one place.
    """

    # The name of the property to install onto every wrapped method
    # Please override, or set `None` if this behavior is undesired
    METHOD_PROPERTY_NAME = 'method_decorator'

    def __init__(self):  # override me to receive arguments
        # Handler method
        self.method = None
        # Handler method function name
        self.method_name = None

    def __call__(self, handler_method):
        # Make sure the object itself is callable only once
        if self.method is not None:
            raise RuntimeError("@{decorator}, when used, is not itself callable".format(decorator=self.__class__.__name__))

        # The handler method to use for saving the field's data
        self.method = handler_method
        self.method_name = handler_method.__name__

        # Store ourselves as a property of the wrapped function :)
        if self.METHOD_PROPERTY_NAME:
            setattr(self.method, self.METHOD_PROPERTY_NAME, self)

        # Use the proper update_wrapper() for we are a decorator
        update_wrapper(self, self.method)

        # Done
        return self  # This is what is saved on the class' __dict__

    def __get__(self, instance, owner):
        """ Magic descriptor: return the wrapped method when accessed """
        # This descriptor magic makes the decorated method accessible directly, even though it's wrapped.
        # This is how it works:
        # whenever a method is wrapped with @saves_relations, there is this decorator class standing in the object's
        # dict instead of the method. The decorator is not callable anymore.
        # However, because it's a descriptor (has the __get__ method), when you access this method
        # (by using class.method or object.method), it will hide itself and give you the wrapped method instead.

        # We, however, will have to pass the `self` argument manually, because this descriptor magic
        # breaks python's passing of `self` to the method
        if instance is None:
            # Accessing a class attribute directly
            # We return the decorator object. It's callable.
            return self

            # Old behavior:
            # # Accessing a class attribute directly
            # # We return the method function, so that subclasses can actually call invoke it unwrapped.
            # return self.method  # got from the class
        else:
            # Accessing an object's attribute
            # We prepare for calling the method.
            return partial(self.method, instance)  # pass the `self`

    def __repr__(self):
        return '@{decorator}({func})'.format(decorator=self.__class__.__name__, func=self.method_name)

    # region: Usage API

    @classmethod
    def is_decorated(cls, method) -> bool:
        """ Check whether the given method is decorated with @cls()

            It also supports detecting methods wrapped with multiple decorators, one of them being @cls.
            Note that it works only when update_wrapper() was properly used.
        """
        return isinstance(method, cls)

    @classmethod
    def get_method_decorator(cls, Klass: type, name: str) -> 'method_decorator':
        """ Get the decorator object, stored as `METHOD_PROPERTY_NAME` on the wrapped method """
        return getattr(getattr(Klass, name), cls.METHOD_PROPERTY_NAME)

    @classmethod
    @lru_cache(256)  # can't be too many views out there.. :)
    def all_decorators_from(cls, Klass: type) -> Iterable['method_decorator']:
        """ Get all decorator objects from a class (cached)

            Note that it won't collect any inherited handler methods:
            only those declared directly on this class.
        """
        if not isinstance(Klass, type):
            raise ValueError('Can only collect decorators from a class, not from an object {}'.format(Klass))

        return tuple(
            attr
            for attr in Klass.__dict__.values()
            if cls.is_decorated(attr))

    # endregion
