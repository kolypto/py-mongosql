from copy import copy


class Reusable:
    """ Make a reusable handler or query

        When a handler object is initialized, it's a pity to waste it!
        This class wrapper makes a copy every time .input() is called on its wrapped object.

        Example:

            project = Reusable(MongoProject(User, force_exclude=('password',))

        It also works for MongoQuery:

            query = Reusable(MongoQuery(User))
    """
    __slots__ = ('__obj',)

    def __init__(self, obj):
        # Just store the object inside
        self.__obj = obj

    # Whenever any attribute (property or method) is accessed, the whole thing is copied.
    # This is copy-on-access

    def __getattr__(self, attr):
        return getattr(copy(self.__obj), attr)

    def __repr__(self):
        return repr(self.__obj)
