import unittest
import re
from copy import copy
from collections import OrderedDict

from mongosql import handlers, MongoQuery, Reusable
from mongosql import InvalidQueryError, DisabledError, InvalidColumnError, InvalidRelationError

from sqlalchemy.orm import Query, Load

from . import models
from .util import q2sql


# Add a custom operator
handlers.MongoFilter.add_scalar_operator('$search', lambda col, val, oval: col.ilike('%{}%'.format(val)))


class QueryStatementsTest(unittest.TestCase):
    """ Test statements as strings """

    longMessage = True

    def test_project(self):
        """ Test project() """
        m = models.User

        project = lambda projection: m.mongoquery().query(project=projection)

        def test_projection(projection, expected_columns):
            """ Test a projection object and see if the resulting SQL query has the expected columns """
            # MongoSQL query done
            mq = project(projection)  # type: mongosql.MongoQuery
            query = mq.end()
            # Test query
            try: test_query(query, expected_columns)
            except:
                print('Projection:', mq.handler_projection.projection)
                print('Full projection:', mq.handler_projection.get_full_projection())
                raise

        def test_query(query, expected_columns):
            """ Test whether an SQL query selects the given set of columns """
            # String query parse
            qs = q2sql(query)
            rex = re.compile('u\.(\w+)[, ]')  # reference to u.id columns
            actual_columns = set(rex.findall(qs))
            # Compare
            self.assertSetEqual(actual_columns,
                                set(expected_columns),
                                'Expected only {} in {}'.format(expected_columns, qs))

        # Empty values
        test_projection(None, ('id', 'name', 'tags', 'age'))
        test_projection([], ('id', 'name', 'tags', 'age'))
        test_projection({}, ('id', 'name', 'tags', 'age'))

        # Array syntax
        test_projection(['id'], ('id',))
        test_projection(['id', 'name'], ('id', 'name'))
        test_projection(['name'], ('id', 'name',))  # PK is always included :)
        with self.assertRaises(InvalidColumnError):
            project(['id', 'lol'])

        # Object: inclusion
        test_projection({'id': 1}, ('id',))
        test_projection({'id': 1, 'name': 1}, ('id', 'name'))
        test_projection({'name': 1}, ('id', 'name',))

        # Object: exclusion
        test_projection({'id': 0}, ('id', 'name', 'tags', 'age'))
        test_projection({'id': 0, 'name': 0}, ('id', 'tags', 'age'))
        test_projection({'name': 0}, ('id', 'tags', 'age'))

        # Object: invalid column
        with self.assertRaises(InvalidColumnError):
            project({'id': 1, 'lol': 1})
        with self.assertRaises(InvalidColumnError):
            project({'id': 0, 'lol': 0})

        # Object: python property
        test_projection({'id': 1, 'user_calculated': 1}, ('id',))  # not in projection because it doesn't have to be loaded

        # PK fun: impossible to exclude the PK from projection
        # This is SqlAlchemy behavior, not MongoSql
        test_projection(['name'], ('id', 'name'))
        test_projection({'name': 1}, ('id', 'name'))
        test_projection({'id': 0}, ('id', 'name', 'tags', 'age'))

    def test_get_project(self):
        # Previously, MongoQuery has a method, get_project(), which allowed to export projections from the query.
        # Now, this method is built into MongoProject.
        # This unit-tests is a test for legacy code compatibility
        m = models.User

        def _get_project(query):
            mq = m.mongoquery().query(**query)
            return mq.handler_project.get_full_projection()

        def _check_query(query, project):
            self.assertEqual(_get_project(query), project)

        _check_query(dict(project=['id', 'name']),
                     {'id': 1, 'name': 1, 'age': 0, 'tags': 0, 'user_calculated': 0})
        _check_query(dict(project={'id': 0, 'name': 0}),
                     {'id': 0, 'tags': 1, 'age': 1, 'name': 0, 'user_calculated': 1})
        _check_query(dict(project={}),
                     {'id': 1, 'tags': 1, 'age': 1, 'name': 1, 'user_calculated': 1})

    def test_sort(self):
        """ Test sort() """
        m = models.User

        sort = lambda sort_spec: m.mongoquery().query(sort=sort_spec).end()

        def test_sort(sort_spec, expected_ends):
            qs = q2sql(sort(sort_spec))
            self.assertTrue(qs.endswith(expected_ends), '{!r} should end with {!r}'.format(qs, expected_ends))

        # Empty
        test_sort(None, u'FROM u')
        test_sort([], u'FROM u')
        test_sort(OrderedDict(), u'FROM u')

        # List
        test_sort(['id-', 'age-'], 'ORDER BY u.id DESC, u.age DESC')

        # Dict
        test_sort(OrderedDict([['id', -1], ['age', -1]]), 'ORDER BY u.id DESC, u.age DESC')

        # Fail
        self.assertRaises(InvalidQueryError, test_sort, OrderedDict([['id', -2], ['age', -1]]), '')

    def test_group(self):
        """ Test group() """
        m = models.User

        group = lambda group_spec: m.mongoquery().query(group=group_spec).end()

        def test_group(group_spec, expected_ends):
            qs = q2sql(group(group_spec))
            self.assertTrue(qs.endswith(expected_ends), '{!r} should end with {!r}'.format(qs, expected_ends))

        # Empty
        test_group(None, u'FROM u')
        test_group([], u'FROM u')
        test_group(OrderedDict(), u'FROM u')

        # List
        test_group(['id-', 'age-'], 'GROUP BY u.id DESC, u.age DESC')

        # Dict
        test_group(OrderedDict([['id', -1], ['age', -1]]), 'GROUP BY u.id DESC, u.age DESC')

        # Fail
        self.assertRaises(InvalidQueryError, test_group, OrderedDict([['id', -2], ['age', -1]]), '')

    def test_filter(self):
        """ Test filter() """
        m = models.User

        filter = lambda criteria: m.mongoquery().query(filter=criteria).end()

        def test_sql_filter(query, expected):
            qs = q2sql(query)
            q_where = qs.partition('\nWHERE ')[2]
            if isinstance(expected, tuple):
                for _ in expected:
                    self.assertIn(_, q_where)
            else:  # string
                self.assertEqual(q_where, expected)

        def test_filter(criteria, expected):
            test_sql_filter(
                filter(criteria),
                expected
            )

        # Empty
        test_filter(None, '')
        test_filter({}, '')

        # Wrong
        self.assertRaises(InvalidQueryError, test_filter, [1, 2], '')

        # Equality, multiple
        test_filter({'id': 1, 'name': 'a'}, ('u.id = 1', 'u.name = a'))
        test_filter({'tags': 'a'}, 'a = ANY (u.tags)')
        test_filter({'tags': ['a', 'b', 'c']}, 'u.tags = CAST(ARRAY[a, b, c] AS VARCHAR[])')

        # $ne
        test_filter({'id': {'$ne': 1}}, 'u.id != 1')
        test_filter({'tags': {'$ne': 'a'}}, 'a != ALL (u.tags)')
        test_filter({'tags': {'$ne': ['a', 'b', 'c']}}, "u.tags != CAST(ARRAY[a, b, c] AS VARCHAR[])")

        # $lt, $lte, $gte, $gt
        test_filter({'id': {'$lt': 1}},  'u.id < 1')
        test_filter({'id': {'$lte': 1}}, 'u.id <= 1')
        test_filter({'id': {'$gte': 1}}, 'u.id >= 1')
        test_filter({'id': {'$gt': 1}},  'u.id > 1')

        # $in
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$in': 1}})
        test_filter({'name': {'$in': ['a', 'b', 'c']}}, 'u.name IN (a, b, c)')
        test_filter({'tags': {'$in': ['a', 'b', 'c']}}, 'u.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])')

        # $nin
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$nin': 1}})
        test_filter({'name': {'$nin': ['a', 'b', 'c']}}, 'u.name NOT IN (a, b, c)')
        test_filter({'tags': {'$nin': ['a', 'b', 'c']}}, 'NOT u.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])')

        # $exists
        test_filter({'name': {'$exists': 0}}, 'u.name IS NULL')
        test_filter({'name': {'$exists': 1}}, 'u.name IS NOT NULL')

        # $all
        self.assertRaises(InvalidQueryError, filter, {'name': {'$all': ['a', 'b', 'c']}})
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$all': 1}})
        test_filter({'tags': {'$all': ['a', 'b', 'c']}}, "u.tags @> CAST(ARRAY[a, b, c] AS VARCHAR[])")

        # $size
        self.assertRaises(InvalidQueryError, filter, {'name': {'$size': 0}})
        test_filter({'tags': {'$size': 0}}, "array_length(u.tags, 1) IS NULL")
        test_filter({'tags': {'$size': 1}}, "array_length(u.tags, 1) = 1")

        # $or
        self.assertRaises(InvalidQueryError, filter, {'$or': {}})
        test_filter({'$or': [{'id': 1}, {'name': 'a'}]}, "(u.id = 1 OR u.name = a)")

        # $and
        self.assertRaises(InvalidQueryError, filter, {'$and': {}})
        test_filter({'$and': [{'id': 1}, {'name': 'a'}]}, "(u.id = 1 AND u.name = a)")

        # $nor
        self.assertRaises(InvalidQueryError, filter, {'$nor': {}})
        test_filter({'$nor': [{'id': 1}, {'name': 'a'}]}, "NOT (u.id = 1 OR u.name = a)")

        # $not
        self.assertRaises(InvalidQueryError, filter, {'$not': []})
        test_filter({'$not': {'id': 1}}, "u.id != 1")

        # Braces
        self.assertRaises(InvalidQueryError, filter, {'$or': {}})
        # "((u.id = 1 AND u.name = a) OR u.name = b)")
        test_filter({'$or': [{'id': 1, 'name': 'a'}, {'name': 'b'}]}, ('u.id = 1', ' AND ', '.name = a', 'OR u.name = b'))

        # Custom filter
        test_filter({'name': {'$search': 'query'}}, 'u.name ILIKE %query%')

        # Filter: Hybrid property
        m = models.Article
        test_filter({'hybrid': True}, (
            # (a.id > 10 AND (EXISTS (SELECT 1
            # FROM u
            # WHERE u.id = a.uid AND u.age > 18))) = true?
            'a.id > 10',
            'u.age > 18'
        ))

        # Filter: Python property (error)
        self.assertRaises(InvalidColumnError, filter, {'calculated': 1})

        # Filter: JSON
        test_filter({'data.rating': {'$gt': 0.5}}, "CAST((a.data #>> ['rating']) AS FLOAT) > 0.5")

    def test_filter_dotted(self):
        """ Test filter(): dotted syntax """
        u = models.User
        c = models.Comment

        # === Test: Dotted syntax
        # Relation: 1-N
        mq = u.mongoquery().query(filter={'articles.id': 1})
        self.assertQuery(mq.end(),
                         "WHERE EXISTS (SELECT 1 \nFROM a \nWHERE u.id = a.uid AND a.id = 1)")

        # Relation: 1-1
        mq = c.mongoquery().query(filter={'user.id': {'$gt': 2}})
        self.assertQuery(mq.end(),
                         "WHERE EXISTS (SELECT 1 \nFROM u \nWHERE u.id = c.uid AND u.id > 2)")

        # Dotted multiple filter for same relation
        mq = c.mongoquery().query(filter={'user.id': {'$gt': 2},
                                          'user.age': {'$gt': 18}})
        self.assertQuery(mq.end(),
                         "WHERE EXISTS (SELECT 1 \nFROM u ",
                         # All conditions nicely grouped into a single subquery
                         "u.id = c.uid",
                         "u.id > 2",
                         "u.age > 18"
                         )

    def test_limit(self):
        """ Test limit() """
        m = models.User

        limit = lambda limit=None, skip=None: m.mongoquery().query(skip=skip, limit=limit).end()

        def test_limit(lim, skip, expected_endswith):
            qs = q2sql(limit(lim, skip))
            self.assertTrue(qs.endswith(expected_endswith), '{!r} should end with {!r}'.format(qs, expected_endswith))

        # Skip
        test_limit(None, None, 'FROM u')
        test_limit(None, -1, 'FROM u')
        test_limit(None, 0, 'FROM u')
        test_limit(None, 1, 'LIMIT ALL OFFSET 1')
        test_limit(None, 9, 'LIMIT ALL OFFSET 9')

        # Limit
        test_limit(-1, None, 'FROM u')
        test_limit(0, None, 'FROM u')
        test_limit(1, None, 'LIMIT 1')
        test_limit(9, None, 'LIMIT 9')

        # Both
        test_limit(5, 10, 'LIMIT 5 OFFSET 10')

        # Twice
        q = limit(limit=10)
        q = m.mongoquery(q).query(limit=15, skip=30).end()
        qs = q2sql(q)
        self.assertTrue(qs.endswith('LIMIT 15 OFFSET 30'), qs)

    def test_aggregate(self):
        """ Test aggregate() """
        m = models.User

        # Configure MongoQuery
        mq = MongoQuery(m, dict(
            aggregateable_columns=('age',),
            aggregate_labels=True,
        ))

        aggregate = lambda agg_spec: copy(mq).query(project=('id',),aggregate=agg_spec).end()

        def test_aggregate(agg_spec, expected_starts):
            qs = q2sql(aggregate(agg_spec))
            self.assertTrue(qs.startswith(expected_starts), '{!r} should start with {!r}'.format(qs, expected_starts))

        def test_aggregate_qs(agg_spec, *expected_query):
            q = aggregate(agg_spec)
            self.assertQuery(q, *expected_query)

        # Empty
        test_aggregate(None, 'SELECT u.id \nFROM')
        test_aggregate({},   'SELECT u.id \nFROM')

        # $func(column)
        test_aggregate({ 'max_age': {'$max': 'age'} }, 'SELECT max(u.age) AS max_age \nFROM')
        test_aggregate({ 'min_age': {'$min': 'age'} }, 'SELECT min(u.age) AS min_age \nFROM')
        test_aggregate({ 'avg_age': {'$avg': 'age'} }, 'SELECT avg(u.age) AS avg_age \nFROM')
        test_aggregate({ 'sum_age': {'$sum': 'age'} }, 'SELECT sum(u.age) AS sum_age \nFROM')

        # $sum(1)
        test_aggregate({'count': {'$sum': 1}}, 'SELECT count(*) AS count')
        test_aggregate({'count': {'$sum': 10}}, 'SELECT count(*) * 10 AS count')

        # $sum(id==1)
        test_aggregate({'count': {'$sum': { 'id': 1 } }}, 'SELECT sum(CAST(u.id = 1 AS INTEGER)) AS count \nFROM')

        # age, $sum(1)
        q = OrderedDict()  # OrderedDict() to have predictable output
        q['age'] = 'age'  # labeled column
        q['n'] = {'$sum': 1}
        test_aggregate_qs(q,
                          'SELECT',
                          'u.age AS age,',
                          'count(*) AS n',
                          'FROM'
                          )

        # $max(age), $sum(id=1 AND age >= 16)
        q = OrderedDict()  # OrderedDict() to have predictable output
        q['max_age'] = {'$max': 'age'}
        q['count'] = {'$sum': OrderedDict([('id', 1), ('age', {'$gte': 16})])}
        test_aggregate_qs(q,
                          'SELECT',
                          'max(u.age) AS max_age,',
                          'sum(CAST((u.id = 1 AND u.age >= 16) AS INTEGER)) AS count',
                          'FROM')

        # Unknown column
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': '???'}, '')
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': {'$max': '???'}}, '')
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': {'$sum': {'???': 1}}}, '')

    def test_count(self):
        """ Test query(count) """
        u = models.User

        # === Test: simple count
        mq = u.mongoquery().query(filter={'age': {'$gt': 18}},
                                  sort=['age-'],
                                  count=True)
        qs = self.assertQuery(mq.end(),
                              # Count
                              'SELECT count(1) AS count_1',
                              # from subquery
                              'FROM (SELECT u.',
                              # From User table
                              'FROM u ',
                              # condition in a subquery
                              'WHERE u.age > 18) AS anon_1',
                              )
        self.assertNotIn('ORDER BY', qs)  # 'count' removed for performance

        # === Test: count with join
        mq = u.mongoquery().query(filter={'age': {'$gt': 18}},
                                  join={'articles': dict(project=['id'],
                                                         filter={'theme': 'sci-fi'})},
                                  count=True)
        self.assertQuery(mq.end(),
                         # Count
                         'SELECT count(1) AS count_1',
                         # Subquery
                         'FROM (SELECT u.',
                         # Join
                         'FROM u LEFT OUTER JOIN a AS a_1 ON u.id = a_1.uid AND a_1.theme = sci-fi',
                         # Filter
                         'WHERE u.age > 18) AS anon_1')

    # ---------- DREADED JOIN LINE ----------
    # Everything below this line is about joins.
    # A lot of blood was spilled on these forgotten fields.

    def assertQuery(self, qs, *expected_lines):
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
                qs = q2sql(qs)

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
        :param expected: list of expected column names
        :returns: query string
        """
        # Query?
        if isinstance(qs, Query):
            qs = q2sql(qs)

        try:
            self.assertEqual(
                self._qs_selected_columns(qs),
                set(expected)
            )
            return qs
        except:
            print(qs)
            raise

    def test_join__one_to_one(self):
        """ Test join() one-to-one """
        a = models.Article

        # === Test: join
        mq = a.mongoquery().query(join=('user',))
        qs = self.assertQuery(mq.end(),
                              "FROM a ",
                              # JOINing an aliased model, with a proper join condition
                              "LEFT OUTER JOIN u AS u_1 "
                              # Proper join condition, also uses aliases
                              "ON u_1.id = a.uid")
        self.assertSelectedColumns(qs, 'a.id', 'a.uid', 'a.title', 'a.theme', 'a.data',
                                   'u_1.id', 'u_1.name', 'u_1.tags', 'u_1.age')

        # === Test: join, projection
        mq = a.mongoquery().query(join={'user': dict(project=['name'])})
        mq.end()

        mq = a.mongoquery().query(
            project=['title'],
            join={'user': dict(project=['name'])}
        )
        qs = self.assertQuery(mq.end(),
                              # outer, aliased, condition also aliased
                              "FROM a LEFT OUTER JOIN u AS u_1 ON u_1.id = a.uid")
        self.assertSelectedColumns(qs,
                                   'a.id',  # PK always included, even if excluded
                                   'a.title',  # 'title', projected
                                   'u_1.id',  # PK always
                                   'u_1.name'  # 'name', projected
                                   # everything else is excluded by projection
                                   )

        # === Test: join, limit
        with self.assertRaises(InvalidQueryError):
            mq = a.mongoquery().query(
                join={'user': dict(limit=10)
            }).end()

        # === Test: join, projection, filter
        mq = a.mongoquery().query(
            project=['title'],
            filter={'data.rating': {'$gt': 0.5}},
            join={'user': dict(project=['name'],
                               filter={'age': {'$gt': 18}})}
        )
        qs = self.assertQuery(mq.end(),
                              # Not testing selected fields anymore
                              # JOIN
                              "FROM a "
                              # properly aliased
                              "LEFT OUTER JOIN u AS u_1 "
                              # join condition uses aliases
                              "ON u_1.id = a.uid "
                              # additional filter put into the ON clause
                              "AND u_1.age > 18",
                              # Filter for the primary entity is put into WHERE
                              "WHERE CAST((a.data #>> ['rating']) AS FLOAT) > 0.5")
        self.assertSelectedColumns(qs,
                                   'a.id', 'a.title',  # PK, projected
                                   'u_1.id', 'u_1.name',  # PK, projected
                                    # nothing else
                                   )

    def test_join__one_to_many(self):
        """ Test: join() one-to-many """
        u = models.User

        # === Test: filter, join, projection
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 18}},
            join={'articles': dict(project=['title'])}
        )
        qs = self.assertQuery(mq.end(),
                              # outer, aliased, condition also aliased
                              "FROM u LEFT OUTER JOIN a AS a_1 ON u.id = a_1.uid",
                              # WHERE condition
                              "WHERE u.age > 18")
        self.assertSelectedColumns(qs,
                                   'u.id', 'u.name',  # PK, projected
                                   'a_1.id', 'a_1.title',  # PK, projected
                                   # nothing else
                                   )

        # === Test: filter, limit, join, projection
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 18}},
            join={'articles': dict(project=['title'])},
            limit=10
        )
        qs = self.assertQuery(mq.end(),
                              # user is a subquery, with the condition and the limit applied to it
                              "FROM (SELECT u.",
                              "WHERE u.age > 18 \n LIMIT 10) AS anon_1",
                              # JOIN condition done properly to a subquery
                              ") AS anon_1 "
                              "LEFT OUTER JOIN a AS a_1 ON anon_1.u_id = a_1.uid")
        self.assertSelectedColumns(qs,
                                   'anon_1.u_id', 'anon_1.u_name',  # PK, projected
                                   'a_1.id', 'a_1.title',  # PK, projected
                                   )

        # === Test: filter, limit, sort, join, projection
        # Try a bunch of different sortings
        for sort_spec, find_in_query in (('age', 'age'), ('age-', 'age DESC'), ('age+', 'age')):
            mq = u.mongoquery().query(
                project=['name'],
                filter={'age': {'$gt': 18}},
                join={'articles': dict(project=['title'])},
                limit=10,
                sort=[sort_spec]
            )
            self.assertQuery(mq.end(),
                             # ordering goes into the subquery as well
                             "WHERE u.age > 18 ORDER BY u.{} \n LIMIT 10) AS anon_1"
                             .format(find_in_query))

        # === Test: filter, join, projection, filter
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 18}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': 'sci-fi'})}
        )
        qs = self.assertQuery(mq.end(),
                              # join, condition
                              "FROM u LEFT OUTER JOIN a AS a_1 ON u.id = a_1.uid AND a_1.theme = sci-fi",
                              # WHERE condition
                              "WHERE u.age > 18"
                              )
        self.assertSelectedColumns(qs,
                                   'u.id', 'u.name',  # PK, projected
                                   'a_1.id', 'a_1.title',  # PK, projected
                                   # nothing else
                                   )

        # === Test: filter, join, filter, sort
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 18}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': 'sci-fi'},
                                   sort=['title+'])},
            sort=['age-']
        )
        self.assertQuery(mq.end(),
                         # First: primary model
                         # Second: related model
                         "ORDER BY u.age DESC, a_1.title"
                         )

        # === Test: filter, limit, join, filter, sort
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 18}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': 'sci-fi'},
                                   sort=['title+'])},
            sort=['age-'],
            limit=10
        )
        self.assertQuery(mq.end(),
                         # First: primary model (must still be first, not in a subquery!)
                         # Second: related model
                         "ORDER BY anon_1.u_age DESC, a_1.title"
                         )

        # === Test: 2 joins, filters and projections
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': 18},
            join={'articles': dict(project=['title'],
                                   filter={'theme': 'sci-fi'},
                                   join={'comments': dict(project=['aid'],
                                                          filter={'text': {'$exists': True}})})}
        )
        qs = self.assertQuery(mq.end(),
                              # Proper join condition
                              "FROM u ",
                              "LEFT OUTER JOIN a AS a_1 "
                                "ON u.id = a_1.uid AND a_1.theme = sci-fi ",
                              "LEFT OUTER JOIN c AS c_1 "
                                "ON a_1.id = c_1.aid AND c_1.text IS NOT NULL"
                              )
        self.assertSelectedColumns(qs,
                                   'u.id', 'u.name',  # PK, projected
                                   'a_1.id', 'a_1.title',  # PK, projected
                                   'c_1.id', 'c_1.aid'  # PK, projected
                                   # nothing else
                                   )

    def test_join__many_to_many(self):
        """ Test join many-to-many """
        g = models.GirlWatcher

        # === Test: no join
        mq = g.mongoquery().query(project=['name'])
        self.assertQuery(mq.end(),
                         'FROM gw')

        # === Test: join, simple (using sqlalchemy options)
        mq = g.mongoquery().query(project=['name'],
                                  join=('best',))
        qs = self.assertQuery(mq.end(),
                              'FROM gw')
        self.assertSelectedColumns(qs,
                                   'gw.id', 'gw.name'
                                   # No other columns, because MongoSql uses selectinload() here
                                   )

        # === Test: join, with filter. Custom query. JOIN.
        mq = g.mongoquery().query(project=['name'],
                                  join={'best': dict(
                                      project=['name'],
                                      filter={'age': {'$gt': 18}}
                                  )})
        qs = self.assertQuery(mq.end(),
                              # Join through an intermediate table
                              'FROM gw '
                              'LEFT OUTER JOIN ('
                                  'gwf AS gwf_1 '
                                  'JOIN u AS u_1 '
                                  'ON gwf_1.user_id = u_1.id) '
                              'ON gw.id = gwf_1.gw_id '
                              # custom query, adapted just fine
                              'AND gwf_1.best = true '
                              # filter condition
                              'AND u_1.age > 18')
        self.assertSelectedColumns(qs,
                                   'gw.id', 'gw.name',
                                   'u_1.id', 'u_1.name',
                                   )

    def test_joinf(self):
        """ Test joinf """
        u = models.User

        # === Test: joinf x 2
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': 18},
            joinf={'articles': dict(project=['title'],
                                    filter={'theme': 'sci-fi'},
                                    joinf={'comments': dict(project=['aid'],
                                                            filter={'text': {'$exists': True}})})}
        )
        self.assertQuery(mq.end(),
                         # Proper join condition
                         "FROM u "
                         "JOIN a AS a_1 ON u.id = a_1.uid "
                         "JOIN c AS c_1 ON a_1.id = c_1.aid",
                         "WHERE u.age = 18 "
                         "AND a_1.theme = sci-fi "
                         "AND c_1.text IS NOT NULL"
                         )

    def test_mongoquery_settings(self):
        """ Test nested MongoQuery settings """
        a = models.Article
        u = models.User
        c = models.Comment
        e = models.Edit

        # === Initialize the settings
        # It will have plenty of configuration
        article_settings = dict(
            force_exclude=('data',),  # projection won't be able to get it
            aggregate=False,  # aggregation disabled
            # Configure queries on related models
            related={
                'user': lambda: user_settings,  # recursively reuse the same configuration
                'comments': lambda: comment_settings
            }
        )

        user_settings = dict(
            aggregateable_columns=('age',),  # can aggregate on this column
            force_include=('name',),  # 'name' is always included
            banned_relations=('roles',),  # a relation is banned
            # Related models
            related={
                'articles': lambda: article_settings,  # recursive config
                'comments': lambda: comment_settings,  # recursive config
            }
        )

        comment_settings = dict(
            # Joins disabled, aggregation disabled
            join=False,
            # joinf=False, # implicitly disabled. don't have to do it
            aggregate=False
            # Everything else is allowed
        )

        edit_settings = dict(
            # When loading users through the edit, restrictions apply
            # Imagine that we want to exclude `password`, or something sensitive like this
            related={
                'user': dict(
                    force_exclude=('tags',)  # sensitive data not allowed
                ),
                'creator': dict(
                    force_exclude=('tags',)  # sensitive data not allowed
                ),
            }
        )

        # The right way would be to configure them globally this way:
        #         a.mongoquery_configure(article_settings)
        #         u.mongoquery_configure(user_settings)
        #         c.mongoquery_configure(comment_settings)
        #         e.mongoquery_configure(edit_settings)
        # But we can't do this, because the test environment has to be reusable.
        # If we configure them globally, other tests would fail.

        a_mq = Reusable(MongoQuery(a, article_settings))
        u_mq = Reusable(MongoQuery(u, user_settings))
        c_mq = Reusable(MongoQuery(c, comment_settings))
        e_mq = Reusable(MongoQuery(e, edit_settings))

        # === Test: Article: force_exclude
        # `force_exclude` on Article won't let us select Article.data
        mq = a_mq.query(project=('title', 'data'))
        self.assertSelectedColumns(mq.end(), 'a.id', 'a.title')  # no `a.data`

        # === Test: Article: aggregate=False
        # aggregation is disabled for Article, and must raise an exception
        with self.assertRaises(DisabledError):
            a_mq.query(aggregate='whatever')

        # === Test: Article: allowed_relations
        # Article only lets you join to 'user' and 'comments'
        mq = a_mq.query(join=('user',))
        self.assertQuery(mq.end(), 'LEFT OUTER JOIN u')  # joined

        # === Test: Article: user: aggregateable_columns
        # can't test: joins don't support aggregation yet

        # === Test: Article: user:  force_include
        # For `user`, you will always get the 'name' column. Always.
        mq = a_mq.query(project=('data',),
                        join={'user': dict(project=('age',))})
        self.assertSelectedColumns(mq.end(),
                                   'a.id',  # `data` excluded (force_exclude)
                                   'u_1.id', 'u_1.age',  # PK, projected
                                   'u_1.name'  # force_include
                                   )

        # === Test: Article: user:  banned_relations
        # For `user`, 'roles' relation is inaccessible
        a_mq.query(join={'user': dict(join=('comments',))})  # okay

        with self.assertRaises(DisabledError):
            a_mq.query(join={'user': dict(join=('roles',))})

        # === Test: Article: user -> articles -> user: force_include
        # This tests a configuration which is triple recursive.
        # Settings must still apply all over the path: force_exclude and force_include
        mq = a_mq.query(project=['title', 'data'],
                        join={'user':
                                  dict(project=['id'],
                                       join={'articles':
                                                 dict(project=['title', 'data'],
                                                      join={'user':
                                                                dict(project=['id'])
                                                            })})})
        self.assertSelectedColumns(mq.end(),
                                   'a.id', 'a.title',  # force_exclude data
                                   'u_1.id', 'u_1.name',  # force_include name
                                   'a_1.id', 'a_1.title',  # force_exclude data
                                   'u_2.id', 'u_2.name',  # force_include name
                                   )

        # === Test: Article: user -> comments: join=False
        # joining is disabled on `comments`
        a_mq.query(join={'user': dict(join={'comments': dict(project=['id',])})})  # ok

        with self.assertRaises(DisabledError):
            a_mq.query(join={'user': dict(join={'comments': dict(join='whatever')})})

        with self.assertRaises(DisabledError):
            # `joinf` implicitly disabled
            a_mq.query(join={'user': dict(join={'comments': dict(joinf='whatever')})})
        
        # === Test: Article: user -> comments: aggregate=False
        # aggregation is disabled for comments
        with self.assertRaises(DisabledError):
            a_mq.query(join={'user': dict(join={'comments': dict(aggregate='whatever')})})

        # === Test: Edit: user -> force_exclude
        mq = e_mq.query(project=['description'],
                        join={'user': dict(# exclude id, name ; left: tags, age
                                           project={'id': 0, 'name': 0}
                                           )})
        self.assertSelectedColumns(mq.end(),
                                   'e.id', 'e.description',  # PK, project
                                   'u_1.id', 'u_1.age',  # +PK ; -tags
                                   )

        # === Test: Articles: user: simple join, force_exclude=('data',)
        # Let's see what happens when we load a relationship with restricted columns without a filter.
        # In this case, MongoJoin will use a different method: pure sqlalchemy loader option.
        # Will it forget to apply our settings?

        special_articles_settings = dict(
            force_exclude=('data',),
            related={
                'user': dict(
                    force_exclude=('tags',)
                )
            }
        )
        special_a_mq = MongoQuery(models.Article, special_articles_settings)

        mq = special_a_mq.query(project=['title', 'data'],
                                join=('user',))  # simple join. Internally, uses joinedload()
        qs = self.assertQuery(mq.end(),
                              # using JOIN
                              'FROM a LEFT OUTER JOIN u')
        self.assertSelectedColumns(qs,
                                   'a.id', 'a.title',  # PK, project, 'data' excluded
                                   'u_1.id', 'u_1.name', 'u_1.age',  # `tags` excluded
                                   'a.uid',  # TODO: FIXME: this column was included by SqlAlchemy? It's not supposed to be here
                                   )

        # === Test: typo in settings
        with self.assertRaises(KeyError):
            MongoQuery(u, dict(
                aggregate=False,
                # a typo
                allowed_Relations=(),
            ))

    # region: Older tests

    def test_join_advanced(self):
        """ Test join()

            These tests are from the older version of MongoSQL and are just adapted for use with the current version.
            Thanks to @vihtinsky for writing them!
        """
        a = models.Article

        # === Test: join x2, project, limit, sort
        for sorting, desc in (('theme', ''), ('theme-', ' DESC'), ('theme+', '')):
            mq = a.mongoquery().query(project=['title'],
                                      join={'comments': dict(project=['aid'],
                                                             join={'user': dict(project=['name'])})},
                                      limit=2,
                                      sort=[sorting])

            qs = self.assertQuery(mq.end(),
                                  # A subquery
                                  "FROM (SELECT a.",
                                  # Ordering within, LIMIT within
                                  "FROM a ORDER BY a.theme{} \n LIMIT 2) AS anon_1 "
                                  .format(desc),
                                  # Joins outside of the subquery
                                  ") AS anon_1 LEFT OUTER JOIN c AS c_1 ON anon_1.a_id = c_1.aid "
                                  "LEFT OUTER JOIN u AS u_1 ON u_1.id = c_1.uid",
                                  # Another ORDER BY on the outside query
                                  "ORDER BY anon_1.a_theme{}"
                                  .format(desc)
                                  )
            self.assertSelectedColumns(qs,
                                       'anon_1.a_id', 'anon_1.a_title',
                                       'u_1.id', 'u_1.name',
                                       'c_1.id', 'c_1.aid',
                                       )

        # === Test: join x3, project, limit
        mq = a.mongoquery().query(project=['title'],
                                  join={'comments': dict(project=['aid'],
                                                         join={'user': dict(project=['name'],
                                                                            join={'roles': dict(project=['title'])})})},
                                  limit=2)
        qs = self.assertQuery(mq.end(),
                              # Subquery, LIMIT
                              "FROM (SELECT a.",
                              "FROM a \nLIMIT 2) AS anon_1",
                              # Joins
                              "LEFT OUTER JOIN c AS c_1 ON anon_1.a_id = c_1.aid "
                              "LEFT OUTER JOIN u AS u_1 ON u_1.id = c_1.uid "
                              "LEFT OUTER JOIN r AS r_1 ON u_1.id = r_1.uid",
                              )
        self.assertSelectedColumns(qs,
                                   'anon_1.a_id', 'anon_1.a_title',
                                   'c_1.id', 'c_1.aid',
                                   'u_1.id', 'u_1.name',
                                   'r_1.id', 'r_1.title'
                                   )

        # More tests
        u = models.User

        # === Test: two joins to the same model
        # Okay
        mq = u.mongoquery().query(join={'articles': dict(project=('title',)), 'comments': dict()})
        self.assertQuery(mq.end(),
                         'FROM u',
                         'LEFT OUTER JOIN a',
                         #'LEFT OUTER JOIN c'  # selectinload() used here, no join
                         )

        # Unknown relation
        mq = u.mongoquery()
        with self.assertRaises(InvalidRelationError):
            mq.query(join=['???'])

    def test_aggregate_and_filter_on_joinf(self):
        u = models.User

        # === Test: aggregate, joinf, filter
        mq = u.mongoquery().query(
            aggregate={'n': {'$sum': 1}},
            group=('name',),
            joinf={'articles': dict(filter={'title': {'$exists': True}})}
        )
        self.assertQuery(mq.end(),
                         # Aggregate ok
                         "SELECT count(*) AS n",
                         # Join ok
                         "FROM u JOIN a AS a_1 ON u.id = a_1.uid",
                         # Condition ok
                         "WHERE a_1.title IS NOT NULL",
                         # Grouping ok
                         "GROUP BY u.name")

    def test_join_multiple_relationships(self):
        """ Test join() same table multiple times"""
        e = models.Edit

        # === Test: join to multiple relationships
        mq = e.mongoquery().query(project=['description'],
                                  join={'user': dict(project=['name']),
                                        'creator': dict(project=['tags'],
                                                        filter={'id': {'$lt': 1}})})
        qs = self.assertQuery(mq.end(),
                              "FROM e ",
                              "LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid ",
                              "LEFT OUTER JOIN u AS u_2 ON u_2.id = e.cuid AND u_2.id < 1"
                              )
        self.assertSelectedColumns(qs,
                                   'u_1.id', 'u_1.name',
                                   'u_2.id', 'u_2.tags',
                                   'e.id', 'e.description'
                                   )

    def test_limit_with_filtered_join(self):
        u = models.User

        mq = u.mongoquery().query(limit=10,
                                  join={'articles': dict(filter={'title': {'$exists': True}})})
        self.assertQuery(mq.end(),
                         "FROM (SELECT u.",
                         "FROM u\n LIMIT 10",
                         "LIMIT 10) AS anon_1 "
                         "LEFT OUTER JOIN a AS a_1 "
                            "ON anon_1.u_id = a_1.uid AND a_1.title IS NOT NULL"
                         )

    # endregion
