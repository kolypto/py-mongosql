from __future__ import absolute_import

from .base import MongoQueryHandlerBase
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoLimit(MongoQueryHandlerBase):
    """ MongoDB limits and offsets

        Handles two keys:
        * 'limit': None, or int: LIMIT for the query
        * 'offset': None, or int: OFFSET for the query
    """

    query_object_section_name = 'sort'

    def __init__(self, model, max_limit=None):
        """ Init a limit

        :param model: Sqlalchem model to work with
        :param max_limit: Upper limit on `limit`: The value can never go any higher.
        """
        super(MongoLimit, self).__init__(model)

        # Config
        self.max_limit = max_limit
        assert self.max_limit is None or self.max_limit > 0

        # On input
        self.skip = None
        self.limit = None

    def input_prepare_query_object(self, query_object):
        """ Alter Query Object

        Unlike other handlers, this one receives 2 values: 'skip' and 'limit'.
        MongoQuery only supports one key per handler.
        Solution: pack them as a tuple
        """
        # (skip, limit) hack
        # LimitHandler is the only one that receives two arguments instead of one.
        # Collect them, and rename
        if 'skip' in query_object or 'limit' in query_object:
            query_object['limit'] = (query_object.pop('skip', None),
                                     query_object.pop('limit', None))
            if query_object['limit'] == (None, None):
                query_object.pop('limit')  # remove it if it's actually empty
        return query_object

    def input(self, skip=None, limit=None):
        # MongoQuery actually gives us a tuple (skip, limit)
        # Adapt.
        if isinstance(skip, tuple):
            skip, limit = skip

        # Super
        super(MongoLimit, self).input((skip, limit))

        # Validate
        if not isinstance(skip, (int, NoneType)):
            raise InvalidQueryError('Skip must be either an integer, or null')
        if not isinstance(limit, (int, NoneType)):
            raise InvalidQueryError('Limit must be either an integer, or null')

        # Clamp
        skip = None if skip is None or skip <= 0 else skip
        limit = None if limit is None or limit <= 0 else limit

        # Max limit
        if self.max_limit:
            limit = min(self.max_limit, limit or self.max_limit)

        # Done
        self.skip = skip
        self.limit = limit
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
        if self.skip:
            query = query.offset(self.skip)
        if self.limit:
            query = query.limit(self.limit)
        return query


NoneType = type(None)
