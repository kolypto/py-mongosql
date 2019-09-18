"""
### Slice Operation
Slicing corresponds to the `LIMIT .. OFFSET ..` part of an SQL query.

The Slice operation consists of two optional parts:

* `limit` would limit the number of items returned by the API
* `skip` would shift the "window" a number of items

Together, these two elements implement pagination.

Example:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    limit: 100, // 100 items per page
    skip: 200,  // skip 200 items, meaning, we're on the third page
}))
```

Values: can be a number, or a `null`.
"""

from sqlalchemy import inspect
from sqlalchemy.sql import func, literal_column

from .base import MongoQueryHandlerBase
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoLimit(MongoQueryHandlerBase):
    """ MongoDB limits and offsets

        Handles two keys:
        * 'limit': None, or int: LIMIT for the query
        * 'offset': None, or int: OFFSET for the query
    """

    query_object_section_name = 'limit'

    def __init__(self, model, bags, max_items=None):
        """ Init a limit

        :param model: Sqlalchemy model to work with
        :param bags: Model bags
        :param max_items: The maximum number of items that can be loaded with this query.
            The user can never go any higher than that, and this value is forced onto every query.
        """
        super(MongoLimit, self).__init__(model, bags)

        # Config
        self.max_items = max_items
        assert self.max_items is None or self.max_items > 0

        # On input
        self.skip = None
        self.limit = None

        # Internal
        # List of columns to group results with (in order to import a limit per group)
        self._window_over_columns = None

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

        # When there is a 'count', we have to disable self.max_items
        # We can safely just alter ourselves, because we're a copy anyway
        if query_object.get('count', False):
            self.max_items = None

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
        if self.max_items:
            limit = min(self.max_items, limit or self.max_items)

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

    @property
    def has_limit(self):
        """ Check thether there's a limit on this handler """
        return self.limit is not None or self.skip is not None

    def limit_groups_over_columns(self, fk_columns):
        """ Instead of the usual limit, use a window function over the given columns.

        This method is used by MongoJoin when doing a custom selectinquery() to load a limited number of related
        items per every primary entity.

        Instead of using LIMIT, LimitHandler will group rows over `fk_columns`, and impose a limit per group.
        This is used to load related models with selectinquery(), where you can now put a limit per group:
        that is, a limit on the number of related entities per primary entity.

        This is achieved using a Window Function:

            SELECT *, row_number() OVER(PARTITION BY author_id) AS group_row_n
            FROM articles
            WHERE group_row_name < 10

            This will result in the following table:

            id  |   author_id   |   group_row_n
            ------------------------------------
            1       1               1
            2       1               2
            3       2               1
            4       2               2
            5       2               3
            6       3               1
            7       3               2

            That's what window functions do: they work like aggregate functions, but they don't group rows.

        :param fk_columns: List of foreign key columns to group with
        """
        # Adaptation not needed, because this method is never used with aliases
        # pa_insp = inspect(self.model)
        # fk_columns = [col.adapt_to_entity(pa_insp) for col in fk_columns]
        assert not inspect(self.model).is_aliased_class, "Cannot be used with aliases; not implemented yet (because nobody needs it anyway!)"

        self._window_over_columns = fk_columns

    def alter_query(self, query, as_relation=None):
        """ Apply offset() and limit() to the query """
        if not self._window_over_columns:
            # Use the regular skip/limit
            if self.skip:
                query = query.offset(self.skip)
            if self.limit:
                query = query.limit(self.limit)
            return query
        else:
            # Use a window function
            return self._limit_using_window_function(query)

    def _limit_using_window_function(self, query):
        """ Apply a limit using a window function

            This approach enables us to limit the number of eagerly loaded related entities
        """
        # Only do it when there is a limit
        if self.skip or self.limit:
            # First, add a row counter:
            query = query.add_columns(
                # for every group, count the rows with row_number().
                func.row_number().over(
                    # Groups are partitioned by self._window_over_columns,
                    partition_by=self._window_over_columns,
                    # We have to apply the same ordering from the outside query;
                    # otherwise, the numbering will be undetermined
                    order_by=self.mongoquery.handler_sort.compile_columns()
                ).label('group_row_n')  # give it a name that we can use later
            )

            # Now, make ourselves into a subquery
            query = query.from_self()

            # Well, it turns out that subsequent joins somehow work.
            # I have no idea how, but they do.
            # Otherwise, we would have had to ban using 'joins' after 'limit' in nested queries.

            # And apply the LIMIT condition using row numbers
            # These two statements simulate skip/limit using window functions
            if self.skip:
                query = query.filter(literal_column('group_row_n') > self.skip)
            if self.limit:
                query = query.filter(literal_column('group_row_n') <= ((self.skip or 0) + self.limit))

        # Done
        return query

    def get_final_input_value(self):
        return dict(skip=self.skip, limit=self.limit)

NoneType = type(None)
