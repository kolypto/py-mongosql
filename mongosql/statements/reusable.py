from copy import copy


class Reusable(object):
    """ Make a reusable statement

        When a statement object is initialized, it's a pity to waste it!
        This class wrapper makes a copy every time .input() is called on its wrapped object.

        Example:

            projection = Reusable(MongoProjection(User, force_exclude=('password',))
    """
    def __init__(self, statement):
        self._statement = statement

    def input(self, qo_value):
        return copy(self._statement).input(qo_value)
