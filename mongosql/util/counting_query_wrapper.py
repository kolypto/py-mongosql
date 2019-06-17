import itertools

from sqlalchemy import func
from sqlalchemy.orm import Query, Session


class CountingQuery:
    """ `Query` object wrapper that can count the rows while returning results

        This is achieved by SELECTing like this:

            SELECT *, count(*) OVER() AS full_count

        In order to be transparent, this class eliminates all those tuples in results and still returns objects
        like a normal query would. The total count is available through a property.

        Example:

            ```python
            qc = CountingQuery(ssn.query(...))

            # Get the count
            qc.count  # -> 127

            # Get the results
            list(qc)

            # (!) only one SQL query was made
            ```
    """
    __slots__ = ('_query', '_original_query',
                 '_count', '_query_iterator',
                 '_single_entity', '_row_fixer')

    def __init__(self, query: Query):
        # The original query. We store it just in case.
        self._original_query = query

        # The current query
        # It differs from the originla query in that it is modified with a window function counting the rows
        self._query = query

        # The iterator for query results ; `None` if the query has not yet been executed
        # If the query has been executed, there is always an iterator available, even if there were no results
        self._query_iterator = None

        # The total count ; `None` if the query has not yet been executed
        self._count = None

        # Whether the query is going to return single entities
        self._single_entity = (  # copied from sqlalchemy.orm.loading.instances
            not getattr(query, '_only_return_tuples', False)  # accessing protected properties
            and len(query._entities) == 1
            and query._entities[0].supports_single_entity
        )

        # The method that will fix result rows
        self._row_fixer = self._fix_result_tuple__single_entity if self._single_entity else self._fix_result_tuple__tuple

    def with_session(self, ssn: Session):
        """ Return a `Query` that will use the given `Session`. """
        self._query = self._query.with_session(ssn)
        return self

    @property
    def count(self):
        """ Get the total count

            If the query has not been executed yet, it will be at this point.
            If there are no rows, it will make an additional query to make sure the result is available.
        """
        # Execute the query and get the count
        if self._count is None:
            self._get_query_count()

        # Done
        return self._count

    def __iter__(self):
        """ Get Query results """
        # Make sure the Query is executed
        if self._query_iterator is None:
            self._query_execute()

        # Iterate
        return self._query_iterator

    # region Counting logic

    def _get_query_count(self):
        """ Retrieve the first row and get the count.
            If that fails due to an OFFSET being present in the query, make an additional, COUNT query.
        """
        # Make a new query
        self._query = self._query.add_column(
            func.count().over()  # this window function will count all rows
        )

        # Execute it
        qi = iter(self._query)

        # Attempt to retrieve the first row
        try:
            first_row = next(qi)
        except StopIteration:
            # No rows in the result.

            # Prepare the iterator anyway
            self._query_iterator = iter(())  # empty iterator

            # If there was an OFFSET in the query, we may have failed because of it.
            if not self._query_has_offset():
                # If there was no offset, then the count is simply zero
                self._count = 0
            else:
                # A separate COUNT() query will do better than us
                self._count = self._get_query_count__make_another_query()

            # Done here
            return

        # Alright, there are some results

        # Get the count
        self._count = self._get_count_from_result_tuple(first_row)

        # Build an iterator that will yield normal result rows
        self._query_iterator = map(
            # The callback that will drop the extra count column
            self._row_fixer,
            itertools.chain(
                # Prepend the first row we're taken off
                [first_row],
                # Add the rest of the results
                qi
            )
        )

    _query_execute = _get_query_count  # makes more sense when called this way in the context of __iter__ method

    def _get_query_count__make_another_query(self) -> int:
        """ Make an additional query to count the number of rows """
        # Build the query
        q = self._original_query

        # Remove eager loads
        q = q.enable_eagerloads(False)

        # Remove LIMIT and OFFSET
        q = q.limit(None).offset(None)

        # Exec
        return q.count()

    def _query_has_offset(self) -> bool:
        """ Tell if the query has an OFFSET clause

            The issue is that with an OFFSET large enough, our window function won't have any rows to return its
            result with. Therefore, we'd be forced to make an additional query.
        """
        return self._query._offset is not None  # accessing protected property

    # endregion

    # region Result tuple processing

    @staticmethod
    def _get_count_from_result_tuple(row):
        """ Get the count from the result row """
        return row[-1]

    @staticmethod
    def _fix_result_tuple__single_entity(row):
        """ Fix the result tuple: get the first Entity only """
        return row[0]

    @staticmethod
    def _fix_result_tuple__tuple(row):
        """ Fix the result tuple: drop the last item """
        return row[:-1]

    # endregion

