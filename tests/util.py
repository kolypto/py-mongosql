import re
import sys

from sqlalchemy import event
from sqlalchemy.orm import Query
from sqlalchemy.dialects import postgresql as pg

PY2 = sys.version_info[0] == 2


def _insert_query_params(statement_str, parameters, dialect):
    """ Compile a statement by inserting *unquoted* parameters into the query """
    return statement_str % parameters


def stmt2sql(stmt, *, literal: bool = False):
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    dialect = pg.dialect()
    query = stmt.compile(
        dialect=dialect,
        compile_kwargs={
            'literal_binds': literal,
        }
    )
    return _insert_query_params(query.string, query.params, pg.dialect())


def q2sql(q, *, literal: bool = False):
    """ Convert an SqlAlchemy query to string """
    return stmt2sql(q.statement, literal=literal)


class TestQueryStringsMixin:
    """ unittest mixin that will help testing query strings """

    def assertQuery(self, qs, *expected_lines, literal: bool = False):
        """ Compare a query line by line

            Problem: because of dict disorder, you can't just compare a query string: columns and expressions may be present,
            but be in a completely different order.
            Solution: compare a query piece by piece.
            To achieve this, you've got to feed the query as a string where every logical piece
            is separated by \n, and we compare the pieces.
            It also removes trailing commas.

            :param expected_lines: the query, separated into pieces
        """
        try:
            # Query?
            if isinstance(qs, Query):
                qs = q2sql(qs, literal=literal)

            # tuple
            expected_lines = '\n'.join(expected_lines)

            # Test
            for line in expected_lines.splitlines():
                self.assertIn(line.strip().rstrip(','), qs)

            # Done
            return qs
        except:
            print(qs)
            raise

    @staticmethod
    def _qs_selected_columns(qs):
        """ Get the set of column names from the SELECT clause

            Example:
            SELECT a, u.b, c AS c_1, u.d AS u_d
            -> {'a', 'u.b', 'c', 'u.d'}
        """
        rex = re.compile(r'^SELECT (.*?)\s+FROM')
        # Match
        m = rex.match(qs)
        # Results
        if not m:
            return set()
        selected_columns_str = m.group(1)
        # Match results
        rex = re.compile(r'(\S+?)(?: AS \w+)?(?:,|$)')  # column names, no 'as'
        return set(rex.findall(selected_columns_str))

    def assertSelectedColumns(self, qs, *expected):
        """ Test that the query has certain columns in the SELECT clause

        :param qs: Query | query string
        :param expected: list of expected column names. Use `None` for a skip
        :returns: query string
        """
        # Query?
        if isinstance(qs, Query):
            qs = q2sql(qs)

        try:
            self.assertEqual(
                self._qs_selected_columns(qs),
                set(expected) - {None},  # exclude the skip
            )
            return qs
        except:
            print(qs)
            raise


class QueryCounter:
    """ Counts the number of queries """

    def __init__(self, engine):
        super(QueryCounter, self).__init__()
        self.engine = engine
        self.n = 0

    def start_logging(self):
        event.listen(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler, named=True)

    def stop_logging(self):
        event.remove(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler)
        self._done()

    def _done(self):
        """ Handler executed when logging is stopped """

    def _after_cursor_execute_event_handler(self, **kw):
        self.n += 1

    def print_log(self):
        pass  # nothing to do

    # Context manager

    def __enter__(self):
        self.start_logging()
        return self

    def __exit__(self, *exc):
        self.stop_logging()
        if exc != (None, None, None):
            self.print_log()
        return False


class QueryLogger(QueryCounter, list):
    """ Log raw SQL queries on the given engine """

    def _after_cursor_execute_event_handler(self, **kw):
        super(QueryLogger, self)._after_cursor_execute_event_handler()
        # Compile, append
        self.append(_insert_query_params(kw['statement'], kw['parameters'], kw['context']))

    def print_log(self):
        for i, q in enumerate(self):
            print('=' * 5, ' Query #{}'.format(i))
            print(q)


class ExpectedQueryCounter(QueryLogger):
    """ A QueryLogger that expects a certain number of queries, raises an error otherwise """

    def __init__(self, engine, expected_queries, comment):
        super(ExpectedQueryCounter, self).__init__(engine)
        self.expected_queries = expected_queries
        self.comment = comment

    def _done(self):
        if self.n != self.expected_queries:
            self.print_log()
            raise AssertionError('{} (expected {} queries, actually had {})'
                                 .format(self.comment, self.expected_queries, self.n))

