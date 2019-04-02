from __future__ import absolute_import

from sqlalchemy import func

from .base import MongoQueryHandlerBase
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoCount(MongoQueryHandlerBase):
    """ MongoDB count query

        Just give it:
        * count=True
    """

    query_object_section_name = 'count'

    def __init__(self, model):
        """ Init a count

        :param model: Sqlalchemy model to work with
        """
        super(MongoCount, self).__init__(model)

        # On input
        self.count = None

    def input_prepare_query_object(self, query_object):
        # When we count, we don't care about certain things
        if query_object.get('count', False):
            # Performance: do not sort when counting
            query_object.pop('sort', None)
            # We don't care about projections either
            query_object.pop('project', None)
            # Also, remove all skips & limits
            query_object.pop('skip', None)
            query_object.pop('limit', None)
            # Finally, when we count, we have to remove `max_items` setting from MongoLimit.
            # Only MongoLimit can do it, and it will do it for us.
            # See: MongoLimit.input_prepare_query_object

        return query_object

    def input(self, count=None):
        super(MongoCount, self).input(count)
        if not isinstance(count, (int, bool, NoneType)):
            raise InvalidQueryError('Count must be either true or false. Or at least a 1, or a 0')

        # Done
        self.count = count
        return self

    def _get_supported_bags(self):
        return None  # not used by this class

    # Not Implemented for this Query Object handler
    compile_columns = NotImplemented
    compile_options = NotImplemented
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation=None):
        """ Apply offset() and limit() to the query """
        if self.count:
            query = query.from_self(func.count(1))
        return query


NoneType = type(None)
