import re
import sys
import unittest
from copy import copy
from collections import OrderedDict

from sqlalchemy import inspect
from sqlalchemy.orm import aliased

from mongosql import handlers, MongoQuery, Reusable, MongoQuerySettingsDict
from mongosql import InvalidQueryError, DisabledError, InvalidColumnError, InvalidRelationError


from . import models
from .util import q2sql, QueryLogger, TestQueryStringsMixin
from .saversion import SA_SINCE, SA_UNTIL, SA_12, SA_13, SA_14


# SqlAlchemy version (see t_selectinquery_test.py)
TEST_QUERY_STRING_ONLY_MATCHES_SA13 = 'This test is skipped in SA 1.2.x entirely, because it works, but builds queries differently'


# Add a custom operator
# We do it globally here; ideally, these should be in the settings
handlers.MongoFilter.add_scalar_operator('$search', lambda col, val, oval: col.ilike('%{}%'.format(val)))


class QueryStatementsTest(unittest.TestCase, TestQueryStringsMixin):
    """ Test statements as strings """

    longMessage = True
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        # Some tests actually need a working db connection
        cls.engine, cls.Session = models.get_working_db_for_tests()

    def setUp(self):
        # By default, it is disabled, because most tests use JOINs.
        # Specific tests that expect selectinquery(), will declare it explicitly
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = False

    def assertFinalQueryObject(self: unittest.TestCase, mq: MongoQuery, **expected_query_object):
        """ Compare the given MongoQuery's final Query Object with your expectations """
        return self.assertEqual(
            mq.get_final_query_object(),
            override_default_query_object(**expected_query_object)
        )

    @classmethod
    def tearDownClass(cls):
        # Restore to the original value
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

    def test_sa_mongoquery_reused(self):
        """ Test that MongoSqlBase.mongoquery() gives us a fresh object every time """
        # === Test: copy(MongoQuery) gives a different object
        mq_1 = MongoQuery(models.User)
        mq_2 = copy(mq_1)

        # Different objects
        self.assertIsNot(mq_1, mq_2)

        # Can be reused
        mq_1.query()
        mq_2.query()  # input() would have complained that it can only be used once

        # === Test: mongoquery() gives two different objects
        mq_1 = models.User.mongoquery()
        mq_2 = models.User.mongoquery()
        mq_3 = models.User.mongoquery()
        self.assertIsNot(mq_1, mq_2)
        self.assertIsNot(mq_2, mq_3)
        self.assertIsNot(mq_1, mq_3)

        # === Test: Make sure that MongoQuery.__init__ is not called anymore: a copy is made instead.
        # Remove the __init__ method
        mongoquery_init_backup = MongoQuery.__init__
        MongoQuery.__init__ = None

        # Get a few mongoqueries
        # If an error happens here, it means that multiple MongoQuery objects are intialized.
        # There should only be one per model!
        models.User.mongoquery()
        models.User.mongoquery()

        # Restore
        MongoQuery.__init__ = mongoquery_init_backup

        # === Test: mongoquery() objects can be reused
        # init() would complain if called twice
        mq = models.User.mongoquery().query(filter={'id': 1})
        mq = models.User.mongoquery().query(filter={'id': 2})

        # === Test: aliased() is applied to a copy, and has no effects on other queries
        mq_1 = models.User.mongoquery().aliased(aliased(models.User))
        mq_2 = models.User.mongoquery()
        # not aliased
        self.assertIsNot(mq_1.bags, mq_2.bags)
        self.assertFalse(inspect(mq_2.model).is_aliased_class)

    def test_aliased(self):
        u = models.User
        ua = aliased(models.User)

        with self.assertRaises(TypeError):
            MongoQuery(ua)

        MongoQuery(u).aliased(ua)  # ok

    def test_project(self):
        """ Test project() """
        mq = Reusable(MongoQuery(models.User))

        project = lambda projection: mq.query(project=projection)

        def test_projection(projection, expected_columns, project_func=project):
            """ Test a projection object and see if the resulting SQL query has the expected columns """
            # MongoSQL query done
            mq = project(projection)  # type: mongosql.MongoQuery
            query = mq.end()

            # Test query
            try: test_query(query, expected_columns)
            except:
                print('Query:', q2sql(query))
                print('Projection:', mq.handler_project.projection)
                print('Full projection:', mq.handler_project.get_full_projection())
                raise

        def test_query(query, expected_columns):
            """ Test whether an SQL query selects the given set of columns """
            # String query parse
            qs = q2sql(query)
            rex = re.compile(r'\.(\w+)[, ]')  # reference to u.id columns
            actual_columns = set(rex.findall(qs))
            # Compare
            self.assertSetEqual(actual_columns,
                                set(expected_columns),
                                'Expected only {} in {}'.format(expected_columns, qs))

        # Empty values
        test_projection(None, ('id', 'name', 'tags', 'age', 'master_id'))
        test_projection([], ('id',))  # can't exclude PK from SQL
        test_projection({}, ('id',))

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
        test_projection({'id': 0}, ('id', 'name', 'tags', 'age', 'master_id'))
        test_projection({'id': 0, 'name': 0}, ('id', 'tags', 'age', 'master_id'))
        test_projection({'name': 0}, ('id', 'tags', 'age', 'master_id'))

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
        test_projection({'id': 0}, ('id', 'name', 'tags', 'age', 'master_id'))

        # BUG: With raiseload=True, it was possible to exclude a PK from projection
        test_projection(['name'], ('id', 'name'), project_func=lambda p: MongoQuery(models.User, dict(raiseload=True)).query(project=p))

        # get_final_query_object() test
        self.assertFinalQueryObject(
            mq.query(project={'id': 0}),
            project=dict(id=0, user_calculated=0)
        )
        self.assertFinalQueryObject(
            mq.query(project=['id', 'age']),
            project=dict(id=1, age=1)
        )

        # Object: default projection + deferred() fields
        mq = Reusable(MongoQuery(models.CustomStrategies))

        test_projection(None, ('id', 'login', 'password'))  # 'password' is included, because MongoProject handled it
        test_projection({}, ('id',))


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
                     {'id': 1, 'name': 1, 'age': 0, 'age_in_10': 0, 'master_id': 0, 'tags': 0, 'user_calculated': 0})
        _check_query(dict(project={'id': 0, 'name': 0}),
                     {'id': 0, 'tags': 1, 'age': 1, 'age_in_10': 1, 'master_id': 1, 'name': 0, 'user_calculated': 0})
        _check_query(dict(project={}),
                     {'id': 0, 'tags': 0, 'age': 0, 'age_in_10': 0, 'master_id': 0, 'name': 0, 'user_calculated': 0})

    def test_sort(self):
        """ Test sort() """
        m = models.User

        sort = lambda sort_spec: m.mongoquery().query(sort=sort_spec).end()

        def test_sort(sort_spec, expected_ends):
            qs = q2sql(sort(sort_spec))
            self.assertTrue(qs.endswith(expected_ends), '{!r} should end with {!r}'.format(qs, expected_ends))

        # Empty
        test_sort(None, 'FROM u')
        test_sort([], 'FROM u')
        test_sort(OrderedDict(), 'FROM u')

        # List
        test_sort(['id-', 'age-'], 'ORDER BY u.id DESC, u.age DESC')

        # Dict
        test_sort(OrderedDict([['id', -1], ['age', -1]]), 'ORDER BY u.id DESC, u.age DESC')

        # Fail
        self.assertRaises(InvalidQueryError, test_sort, OrderedDict([['id', -2], ['age', -1]]), '')

        # get_final_query_object() test
        self.assertFinalQueryObject(
            m.mongoquery().query(sort=['id-', 'age+']),
            project=dict(user_calculated=0),
            sort=['id-', 'age']
        )

    def test_group(self):
        """ Test group() """
        m = models.User

        group = lambda group_spec: m.mongoquery().query(group=group_spec).end()

        def test_group(group_spec, expected_ends):
            qs = q2sql(group(group_spec))
            self.assertTrue(qs.endswith(expected_ends), '{!r} should end with {!r}'.format(qs, expected_ends))

        # Empty
        test_group(None, 'FROM u')
        test_group([], 'FROM u')
        test_group(OrderedDict(), 'FROM u')

        # List
        test_group(['id-', 'age-'], 'GROUP BY u.id DESC, u.age DESC')

        # Dict
        test_group(OrderedDict([['id', -1], ['age', -1]]), 'GROUP BY u.id DESC, u.age DESC')

        # Fail
        self.assertRaises(InvalidQueryError, test_group, OrderedDict([['id', -2], ['age', -1]]), '')


        # get_final_query_object() test
        self.assertFinalQueryObject(
            m.mongoquery().query(group=['id-', 'age+']),
            project=dict(user_calculated=0),
            group=['id-', 'age']
        )

    def test_filter(self):
        """ Test filter() """
        m = models.User

        filter = lambda criteria: m.mongoquery().query(filter=criteria).end()

        def test_sql_filter(query, expected, *, literal: bool = False):
            qs = q2sql(query, literal=literal)
            q_where = qs.partition('\nWHERE ')[2]
            if isinstance(expected, tuple):
                for _ in expected:
                    self.assertIn(_, q_where)
            else:  # string
                self.assertEqual(q_where, expected)

        def test_filter(criteria, expected, *, literal: bool = False):
            test_sql_filter(
                filter(criteria),
                expected,
                literal=literal,
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
        test_filter({'id': {'$ne': 1}}, 'u.id IS DISTINCT FROM 1')
        test_filter({'tags': {'$ne': 'a'}}, 'a != ALL (u.tags)')
        test_filter({'tags': {'$ne': ['a', 'b', 'c']}}, "u.tags != CAST(ARRAY[a, b, c] AS VARCHAR[])")

        # $lt, $lte, $gte, $gt
        test_filter({'id': {'$lt': 1}},  'u.id < 1')
        test_filter({'id': {'$lte': 1}}, 'u.id <= 1')
        test_filter({'id': {'$gte': 1}}, 'u.id >= 1')
        test_filter({'id': {'$gt': 1}},  'u.id > 1')

        # $prefix
        test_filter({'name': {'$prefix': 'ma'}}, "(u.name LIKE ma || '%')")

        # $in
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$in': 1}})
        test_filter({'name': {'$in': ['a', 'b', 'c']}}, "u.name IN ('a', 'b', 'c')", literal=True)
        test_filter({'tags': {'$in': ['a', 'b', 'c']}}, "u.tags && CAST(ARRAY['a', 'b', 'c'] AS VARCHAR[])", literal=True)

        # $nin
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$nin': 1}})
        test_filter({'name': {'$nin': ['a', 'b', 'c']}}, "u.name NOT IN ('a', 'b', 'c')", literal=True)
        test_filter({'tags': {'$nin': ['a', 'b', 'c']}}, "NOT u.tags && CAST(ARRAY['a', 'b', 'c'] AS VARCHAR[])", literal=True)

        # $exists
        test_filter({'name': {'$exists': 0}}, 'u.name IS NULL')
        test_filter({'name': {'$exists': 1}}, 'u.name IS NOT NULL')

        # $all
        self.assertRaises(InvalidQueryError, filter, {'name': {'$all': ['a', 'b', 'c']}})
        self.assertRaises(InvalidQueryError, filter, {'tags': {'$all': 1}})
        test_filter({'tags': {'$all': ['a', 'b', 'c']}}, "u.tags @> CAST(ARRAY['a', 'b', 'c'] AS VARCHAR[])", literal=True)

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

        # get_final_query_object() test
        self.assertFinalQueryObject(
            m.mongoquery().query(filter={'$not': {'id': 1}}),
            project=dict(calculated=0, hybrid=0),
            filter={'$not': {'id': 1}},
        )

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

        # get_final_query_object() test
        self.assertFinalQueryObject(
            m.mongoquery().query(limit=100),
            project=dict(user_calculated=0),
            skip=None,
            limit=100
        )

    def test_aggregate(self):
        """ Test aggregate() """
        u = models.User

        # Configure MongoQuery
        mq = MongoQuery(u, MongoQuerySettingsDict(
            aggregate_columns=('age',),
            aggregate_labels=True,
        ))

        aggregate_mq = lambda agg_spec: copy(mq).query(project=('id',),aggregate=agg_spec)

        def test_aggregate(agg_spec, expected_starts, *, literal: bool = False):
            mq = aggregate_mq(agg_spec)
            qs = q2sql(mq.end(), literal=literal)
            self.assertTrue(qs.startswith(expected_starts), '{!r} should start with {!r}'.format(qs, expected_starts))

        def test_aggregate_qs(agg_spec, *expected_query):
            mq = aggregate_mq(agg_spec)
            q = mq.end()
            self.assertQuery(q, *expected_query)
            return mq

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

        # $sum(id==1), bool expression
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
        ag = test_aggregate_qs(q,
                               'SELECT',
                               'max(u.age) AS max_age,',
                               'sum(CAST((u.id = 1 AND u.age >= 16) AS INTEGER)) AS count',
                               'FROM')

        # projection
        self.assertEqual(ag.handler_aggregate.projection, {'count': 1, 'max_age': 1})

        # Unknown column
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': '???'}, '')
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': {'$max': '???'}}, '')
        self.assertRaises(InvalidColumnError, test_aggregate, {'a': {'$sum': {'???': 1}}}, '')

        # aggregate by JSON field
        mq = MongoQuery(models.Article, MongoQuerySettingsDict(
            aggregate_columns=('data',),
            aggregate_labels=True,
        ))

        aggregate_mq = lambda agg_spec: copy(mq).query(project=('id',),aggregate=agg_spec)

        test_aggregate(
            {'max_rating': {'$max': 'data.rating'}},
            "SELECT max(CAST(a.data #>> '{rating}' AS FLOAT)) AS max_rating",
            literal=True
        )

        # aggregate + filter
        # TODO: unit-test

        # get_final_query_object() test
        self.assertFinalQueryObject(
            aggregate_mq({'avg_rating': {'$avg': 'data.rating'}}),
            project=dict(id=1),
            aggregate={'avg_rating': {'$avg': 'data.rating'}}
        )

    def test_invalid__aggregate_with_projection(self):
        """ Invalid combination: aggregate + project """
        u = models.User

        # === Test: aggregate + project
        # It is invalid to use projections with aggregate, because regular columns are not available with projections!
        mq = u.mongoquery().query(  # no error
            aggregate={'n': {'$sum': 1}},
            project=('name',)
        )
        self.assertNotIn('name', q2sql(mq.end()))  # not mentioned

        # === Test: aggregate + settings: force_include
        # Note that when aggregation is used, there may be no projections.
        # The user may understand that; but handler settings may still make it do something.
        # Let's hope this setting will not ruin everything
        # Expected result: MongoProject won't put any options() on the query
        # Possible result:
        #       ArgumentError: Query has only expression-based entities - can't find property named 'articles'.
        mq_user = Reusable(MongoQuery(
            models.User,
            dict(
                force_include=('name',)
            )
        ))

        # === Test: aggregate, joinf, filter
        mq = mq_user.query(  # no error
            aggregate={'n': {'$sum': 1}},
            group=('id',),
        )
        self.assertNotIn('name', q2sql(mq.end()))  # not mentioned

    def test_count(self):
        """ Test query(count) """
        u = models.User

        # === Test: just count
        mq = u.mongoquery().query(count=True)
        self.assertQuery(mq.end(),
                         # The FROM clause must not be missing
                         'FROM u')

        # === Test: count + filter + sort
        mq = u.mongoquery().query(filter={'age': {'$gt': 18}},
                                  sort=['age-'],
                                  count=True)
        # qs = self.assertQuery(mq.end(),  # No more subquery ; see MongoCount.alter_query
        #                       # Count
        #                       'SELECT count(1) AS count_1',
        #                       # from subquery
        #                       'FROM (SELECT u.',
        #                       # From User table
        #                       'FROM u ',
        #                       # condition
        #                       'WHERE u.age > 18) AS anon_1',
        #                       )
        qs = self.assertQuery(mq.end(),
                              # Count
                              'SELECT count(*)',
                              # from table, directly
                              'FROM u ',
                              # condition
                              'WHERE u.age > 18',
                              )
        self.assertNotIn('ORDER BY', qs)  # 'count' removed for performance

        # === Test: count with join
        mq = u.mongoquery().query(filter={'age': {'$gt': 18}},
                                  join={'articles': dict(project=['id'],
                                                         filter={'theme': 'sci-fi'})},
                                  count=True)
        # self.assertQuery(mq.end(),  # No more subquery ; see MongoCount.alter_query
        #                  # Count
        #                  'SELECT count(1) AS count_1',
        #                  # Subquery
        #                  'FROM (SELECT u.',
        #                  # Join
        #                  'FROM u LEFT OUTER JOIN a AS a_1 ON u.id = a_1.uid AND a_1.theme = sci-fi',
        #                  # Filter
        #                  'WHERE u.age > 18) AS anon_1')
        self.assertQuery(mq.end(),
                         # Count
                         'SELECT count(*)',
                         # From table
                         'FROM u',
                         # NOTE: no joins
                         # Filter
                         'WHERE u.age > 18')

        # get_final_query_object() test
        self.assertFinalQueryObject(
            u.mongoquery().query(count=2),
            project=dict(user_calculated=0),
            count=2
        )

    # ---------- DREADED JOIN LINE ----------
    # Everything below this line is about joins.
    # A lot of blood was spilled on these forgotten fields.

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
                                   'u_1.id', 'u_1.name', 'u_1.tags', 'u_1.age', 'u_1.master_id')

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

        # Test __contains__()
        self.assertTrue('user' in mq.handler_join)

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

        # get_final_query_object() test
        self.assertFinalQueryObject(
            mq,
            project={'title': 1},
            filter={'data.rating': {'$gt': 0.5}},
            join={
                'user': override_default_query_object(
                    project={'name': 1},
                    filter={'age': {'$gt': 18}}
                )
            }
        )

    def test_join__one_to_one__twice(self):
        """ Test join() one-to-one, twice """
        c = models.Comment

        # === Test: Make two LEFT JOINs
        query_obj = dict(
            project=['id'],
            join={
                # 1-1, MongoJoin will choose RELSTRATEGY_LEFT_JOIN
                'article': dict(project=['id']),
                # 1-1, MongoJoin will choose RELSTRATEGY_LEFT_JOIN
                'user': dict(project=['id']),
            },
            # two LEFT JOINs here
        )
        mq = c.mongoquery().query(**query_obj)

        qs = self.assertQuery(mq.end(),
                              'FROM c',
                              'LEFT OUTER JOIN a AS a_1 ON a_1.id = c.aid',
                              # 'LEFT OUTER JOIN u AS u_1 ON u_1.id = c.uid'  # not here because selectinload() would handle it
                              )
        self.assertNotIn('JOIN u', qs)  # not here because selectinload() would handle it

        self.assertSelectedColumns(qs,
                                   'c.id',
                                   'a_1.id',
                                   # 'u_1.id',  # not here because selectinload() would handle it
                                   )

        # === Test: same, with LIMIT
        # When MongoSQL used the RELSTRATEGY_LEFT_JOIN with LIMIT, it used to corrupt the query beyond recognition,
        # and the second LEFT JOIN was unable to attach to that mutilated query at all.
        # This test sees what happens if we join two relations by LEFT JOIN
        mq = c.mongoquery().query(
            **query_obj,
            # two LEFT JOINs here
            limit=1
        )

        # This is the sort of query you'd expect if I fixed it properly
        qs = self.assertQuery(mq.end(),
                              'FROM (SELECT c.id',
                              'FROM c',
                              'LIMIT 1) AS anon_1',
                              'LEFT OUTER JOIN a AS a_1 ON a_1.id = anon_1.c_aid',
                              # This second line used to contain a wrong, unaliased ON clause: "ON u_1.id = c.uid"
                              # In fact, I didn't fix it at all ; I moved it into a selectinquery() handler
                              # 'LEFT OUTER JOIN u AS u_1 ON u_1.id = anon_1.c_aid'
                              )
        self.assertNotIn('JOIN u', qs)  # not here because selectinload() would handle it

        self.assertSelectedColumns(qs,
                                   'anon_1.c_id', 'anon_1.c_aid',
                                   'a_1.id',
                                   # 'u_1.id',  # not here because selectinload() would handle it
                                   )

    def test_join__one_to_one__twice__same_model(self):
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
                              # "LEFT OUTER JOIN u AS u_2 ON u_2.id = e.cuid AND u_2.id < 1"  # not here because selectinload() would handle it
                              )
        self.assertNotIn('JOIN u AS u_2', qs)  # not here because selectinload() would handle it
        self.assertSelectedColumns(qs,
                                   'u_1.id', 'u_1.name',
                                   # 'u_2.id', 'u_2.tags',  # not here because selectinload() would handle it
                                   'e.id', 'e.description'
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

    def test_join_self_referential_model(self):
        """ Test joining a relationship that points to the same model """
        u = models.User

        # === Test: load a self-referential relationship
        mq = u.mongoquery().query(
            project=['id', 'master_id'],
            join={'master': dict(project=['id'])}
        )
        qs = self.assertQuery(mq.end(),
                              # Properly aliased
                              'FROM u',
                              'LEFT OUTER JOIN u AS u_1 ON u_1.id = u.master_id'
                              )
        self.assertSelectedColumns(qs,
                                   'u.id', 'u.master_id', 'u_1.id',
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

        # === Test: joinf + LIMIT on the outer query
        mq = u.mongoquery().query(
            project=['name'],
            limit=10,
            joinf={'articles': dict(project=['title'],
                                    filter={'theme': 'sci-fi'},
                                    )}
        )
        self.assertQuery(mq.end(),
                         # Inner query wrapped into a subquery
                         'FROM (SELECT u.id AS u_id, u.name AS u_name',
                         'FROM u',
                         'LIMIT 10) AS anon_1',
                         # Join condition
                         'anon_1 JOIN a AS a_1 ON anon_1.u_id = a_1.uid',
                         # Filter
                         'WHERE a_1.theme = sci-fi'
                         )

    def test_model_with_lazy_relationships(self):
        """ Test how querying a model with relationship(lazy=joined) works """
        ll = models.ConfiguredLazyloadModel

        # === Test: plain query, nothing special
        mq = ll.mongoquery().query()

        self.assertQuery(mq.end().with_labels(),  # with_labels() because of joined tables and ambiguous column1 names
                         'FROM ll '
                         'LEFT OUTER JOIN u AS u_1 ON u_1.id = ll.user_id '
                         'LEFT OUTER JOIN a AS a_1 ON a_1.id = ll.article_id')

        # === Test: with LIMIT and a join
        # Since a LIMIT uses from_self(), we have to test that all these relationships are loaded correctly
        mq = ll.mongoquery().query(join={'comment': dict(project=('text',))},
                                   limit=10)

        self.assertQuery(mq.end(),
                         # Proper subquery
                         'FROM (SELECT ll.id AS ll_id,',
                         'LIMIT 10) AS anon_1',
                         # All LEFT JOINs use aliased subquery: anon_1
                         'LEFT OUTER JOIN c AS c_1 ON c_1.id = anon_1.ll_comment_id '
                         'LEFT OUTER JOIN u AS u_1 ON u_1.id = anon_1.ll_user_id '
                         'LEFT OUTER JOIN a AS a_1 ON a_1.id = anon_1.ll_article_id',
                         )

        # === Test: with LIMIT
        # See how it respects other joins: it's supposed to use from_self()
        # TODO: this test case currently fails: MongoSQL does not support putting a LIMIT on queries with joinedload()
        #   eager loading. Have to implement it inside the LIMIT handler: if there are LEFT JOINs, the LIMIT has to use 
        #   from_self().
        #   However, another option would be to limit using LEFT JOIN only to one-to-one relationships (as we do it now),
        #   and then a LIMIT clause can safely be put onto it.
        # mq = ll.mongoquery().query(limit=10)
        #
        # self.assertQuery(mq.end(),
        #                  # Has to be a subquery, because of other joins put on the query outside of us
        #                  'LIMIT 10) as anon')

    def test_mongoquery_settings(self):
        """ Test nested MongoQuery settings """
        a = models.Article
        u = models.User
        c = models.Comment
        e = models.Edit

        # === Initialize the settings
        # It will have plenty of configuration
        article_settings = MongoQuerySettingsDict(
            force_exclude=('data',),  # projection won't be able to get it
            ensure_loaded=('uid',),  # fields will always be loaded
            aggregate_enabled=False,  # aggregation disabled
            # Configure queries on related models
            related={
                'user': lambda: user_settings,  # recursively reuse the same configuration
                'comments': lambda: comment_settings
            }
        )

        user_settings = MongoQuerySettingsDict(
            aggregate_columns=('age',),  # can aggregate on this column
            force_include=('name',),  # 'name' is always included
            banned_relations=('roles',),  # a relation is banned
            force_filter={'age': {'$gte': 18}},  # whatever you do, you can only get older users
            # Related models
            related={
                'articles': lambda: article_settings,  # recursive config
                'comments': lambda: comment_settings,  # recursive config
            },
        )

        comment_settings = MongoQuerySettingsDict(
            # Joins disabled, aggregation disabled
            join_enabled=False,
            # joinf=False, # implicitly disabled. don't have to do it
            aggregate_enabled=False
            # Everything else is allowed
        )

        edit_settings = MongoQuerySettingsDict(
            # When loading users through the edit, restrictions apply
            # Imagine that we want to exclude `password`, or something sensitive like this
            related={
                'user': dict(
                    force_filter=lambda model: [model.age >= 18],  # whatever you do, you can only get older users
                    force_exclude=('tags',),  # sensitive data not allowed
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

        # === Test: Article: ensure_loaded
        mq = a_mq.query(project=('id',))
        self.assertSelectedColumns(mq.end(), 'a.id', 'a.uid')  # `uid` loaded
        self.assertEqual(mq.get_projection_tree(), dict(id=1))  # `uid` missing from projection

        mq = a_mq.query(project=('id', 'uid'))
        self.assertSelectedColumns(mq.end(), 'a.id', 'a.uid')  # `uid` loaded
        self.assertEqual(mq.get_projection_tree(), dict(id=1, uid=1))  # `uid` now in projection because requested explicitly

        # === Test: Article: force_exclude
        # `force_exclude` on Article won't let us select Article.data
        mq = a_mq.query(project=('title', 'data'))
        # will include `a.uid` because it's ensure_loaded()
        self.assertSelectedColumns(mq.end(), 'a.id', 'a.uid', 'a.title')  # no `a.data`
        self.assertEqual(mq.get_projection_tree(), dict(title=1))  # `uid` missing from projection because it's `ensure_loaded`

        # === Test: Article: aggregate=False
        # aggregation is disabled for Article, and must raise an exception
        with self.assertRaises(DisabledError):
            a_mq.query(aggregate='whatever')

        # === Test: Article: allowed_relations
        # Article only lets you join to 'user' and 'comments'
        mq = a_mq.query(join=('user',))
        self.assertQuery(mq.end(), 'LEFT OUTER JOIN u')  # joined

        # === Test: Article: user: aggregate_columns
        # can't test: joins don't support aggregation yet

        # === Test: Article: user:  force_include
        # For `user`, you will always get the 'name' column. Always.
        mq = a_mq.query(project=('data',),
                        join={'user': dict(project=('age',))})
        self.assertSelectedColumns(mq.end(),
                                   'a.id', 'a.uid',  # PK + ensure_loaded
                                   # `a.data` excluded (force_exclude)
                                   'u_1.id', 'u_1.age',  # PK, projected
                                   'u_1.name'  # force_include
                                   )

        # === Test: User: force_filter + max_items + join
        mq = MongoQuery(u, {**user_settings, 'max_items': 10}).query(
            project=('name',), 
            sort=['age-'],
            join={'articles': {'join':[]}},
        )
        self.assertQuery(mq.end(),
                         'FROM u',
                         # The condition is inserted
                         'WHERE u.age >= 18',
                         'LIMIT 10')

        # === Test: Article: user: force_filter, dict
        mq = a_mq.query(project=('data',),
                        join={'user': dict(project=('age',))})
        self.assertQuery(mq.end(),
                         'FROM a '
                         'LEFT OUTER JOIN u AS u_1 '
                         # The condition is right here, even though it was loaded as a relationship
                            'ON u_1.id = a.uid AND u_1.age >= 18')

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
                                   'a.id', 'a.uid', 'a.title',  # force_exclude data ; ensure_loaded uid
                                   'u_1.id', 'u_1.name',  # force_include name
                                   'a_1.id', 'a_1.uid', 'a_1.title',  # force_exclude data ; ensure_loaded uid
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
        qs = mq.end()
        self.assertSelectedColumns(qs,
                                   'e.id', 'e.description', '10',  # PK, project
                                   'u_1.id', 'u_1.age', 'u_1.master_id',  # +PK ; -tags
                                   )

        # === Test: Edit: user -> force_filter, callable
        self.assertQuery(qs,
                         'FROM e '
                         'LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid '
                         # Callable installed the condition into the ON clause, thus not distorting the results
                         'AND u_1.age >= 18',)

        # === Test: calling mq.end() twice gives you the same result
        self.assertEqual(str(qs), str(mq.end()))

        # === Test: Articles: user: simple join, force_exclude=('data',)
        # Let's see what happens when we load a relationship with restricted columns without a filter.
        # In this case, MongoJoin will use a different method: pure sqlalchemy loader option.
        # Will it forget to apply our settings?

        special_articles_settings = MongoQuerySettingsDict(
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
                                   'u_1.id', 'u_1.name', 'u_1.age', '10', 'u_1.master_id',  # `tags` excluded
                                   'a.uid',  # TODO: FIXME: this column was included by SqlAlchemy? It's not supposed to be here
                                   )

        # === Test: related(force_filter + max_items)
        mq = MongoQuery(models.Article, MongoQuerySettingsDict(
            related={'user': MongoQuerySettingsDict(
                force_filter=lambda User: [User.age > 1]
            )},
            max_items=100
        )).query(project=['title'],
                 join=['user'])

        qs = self.assertQuery(mq.end(),
                              'FROM (SELECT a.',
                              'FROM a',
                              'LIMIT 100) AS anon_1',
                              'anon_1 LEFT OUTER JOIN u AS u_1',
                              'ON u_1.id = anon_1.a_uid AND u_1.age > 1')

        # ###
        # ### Test related settings
        # ###

        # === Test: typo in settings
        with self.assertRaises(KeyError):
            MongoQuery(u, dict(
                aggregate_enabled=False,
                # a typo
                allowed_Relations=(),
            ))

        # === Test: related[*]
        def test_settings_for(mq, relation_name, target_model, expected_settings):
            handler_settings = mq.handler_settings.settings_for_nested_mongoquery(relation_name, target_model)
            self.assertEqual(handler_settings, expected_settings)

        mq = MongoQuery(u, MongoQuerySettingsDict(
            related={
                '*': lambda relation_name, target_model: dict(join=False) if relation_name == 'articles' else None,
            }
        ))

        test_settings_for(mq, 'articles', models.Article,
                          expected_settings=dict(join=False))

        # === Test: related_models
        mq = MongoQuery(u, MongoQuerySettingsDict(
            related_models={
                models.User: user_settings,
                models.Article: article_settings,
                models.Comment: lambda: comment_settings,  # callable
                '*': lambda relation_name, target_model: dict(join=False),  # no more
            }
        ))

        test_settings_for(mq, 'articles', models.Article,
                          expected_settings=article_settings)

        test_settings_for(mq, 'comments', models.Comment,
                          expected_settings=comment_settings)

        test_settings_for(mq, 'roles', models.Role,
                          expected_settings=dict(join=False))

        # === Test: typo in "related" names
        with self.assertRaises(KeyError):
            MongoQuery(u, dict(
                related={
                    'UNK': None,
                }
            ))

        # === Test: using non-models in "related_models"
        with self.assertRaises(KeyError):
            MongoQuery(u, dict(
                related_models={
                    'User': None,
                }
            ))

        with self.assertRaises(KeyError):
            MongoQuery(u, dict(
                related={
                    object: None,
                }
            ))

        # === Test: using related_models to configure a global registry
        user_settings = MongoQuerySettingsDict(
            allowed_relations=('articles',),
            related_models=lambda: model_settings,
        )
        article_settings = MongoQuerySettingsDict(
            allowed_relations=('user',),
            related_models=lambda: model_settings,
        )
        comment_settings = MongoQuerySettingsDict(
            allowed_relations=(),
            related_models=lambda: model_settings,
        )

        model_settings = {
            models.User: user_settings,
            models.Article: article_settings,
            models.Comment: comment_settings,
        }

        mq = MongoQuery(u, user_settings)

        test_settings_for(mq, 'articles', models.Article,
                          expected_settings=article_settings)
        test_settings_for(mq, 'comments', models.Comment,
                          expected_settings=comment_settings)

    def test_mongoquery_settings_with_limit(self):
        """ Test how nested MongoQueries work when they have limit.

        The purpose of this test is to try different ways of loading nested relationships and find errors in how LIMIT works.

        It turned out that one could throw a `limit` into MongoJoin and make it do queries it didn't really support.
        This complicated scenario happened because MongoJoin used to check to `limit` inside the Query Object, but didn't check mongosettings,
        which contained `max_items` that would impose a LIMIT in the end.
        """
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

        engine = self.engine
        ssn = self.Session()

        # === Test: using `limit` in related settings

        # Article
        # Build a tree of relationships so that we can test:
        #   1) joinedload -> selectinquery
        #   2) joinedload -> joinedload
        #   3) selectinquery -> joinedload
        #   4) selectinquery -> selectinquery
        limited_user_settings = MongoQuerySettingsDict(
            max_items=20,
            allowed_relations=['articles'],
            related={
                # User -> Articles: one user has many articles
                # Join strategy: selectinload()
                'articles': lambda: MongoQuerySettingsDict(
                    max_items=10,
                    allowed_relations=['user', 'comments'],
                    related={
                        # Article -> User: one article has one user
                        # Join strategy: joinedload()
                        'user': lambda: MongoQuerySettingsDict(
                            max_items=5,
                        ),
                        # Article -> Comment: one article has many comments
                        # Join strategy: selectinload()
                        'comments': lambda: MongoQuerySettingsDict(
                            max_items=5,
                        ),
                    }
                ),
            }
        )

        limited_comment_settings = MongoQuerySettingsDict(
            max_items=20,
            allowed_relations=['article'],
            related={
                # Comment -> Article: one comment has one article.
                # Join strategy: joinedload()
                'article': lambda: MongoQuerySettingsDict(
                    max_items=10,
                    allowed_relations=['user', 'comments'],
                    related={
                        # Article -> User: one article has one user
                        # Join strategy: joinedload()
                        'user': lambda: MongoQuerySettingsDict(
                            max_items=5,
                        ),
                        # Article -> Comments: one article has many comments
                        # Join strategy: selectinload()
                        'comments': lambda: MongoQuerySettingsDict(
                            max_items=5,
                        ),
                    }
                ),
            }
        )

        limited_u_mq = Reusable(MongoQuery(models.User, limited_user_settings))
        limited_c_mq = Reusable(MongoQuery(models.Comment, limited_comment_settings))

        # ### User -> Article: selectinload()
        with QueryLogger(engine) as ql:
            mq = limited_u_mq.query(join={'articles': dict(project=['id'])})
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0], 'LIMIT 20')
            self.assertQuery(ql[1], 'WHERE group_row_n <= 10')
            self.assertEqual(len(ql), 2)

        with QueryLogger(engine) as ql:
            # Query limit: users
            mq = limited_u_mq.query(join={'articles': dict(project=['id'])}, limit=3)
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0], 'LIMIT 3')  # limit from the Query Object
            self.assertQuery(ql[1], 'WHERE group_row_n <= 10')
            self.assertEqual(len(ql), 2)

        with QueryLogger(engine) as ql:
            # Query limit: articles
            mq = limited_u_mq.query(join={'articles': dict(project=['id'], limit=3)})
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0], 'LIMIT 20')
            self.assertQuery(ql[1], 'WHERE group_row_n <= 3')  # limit from the Query Object
            self.assertEqual(len(ql), 2)

        # ### User -> Article -> User: selectinload() -> joinedload()
        # Will fail: joinedload() does not support LIMIT
        with self.assertRaises(ValueError):
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           join={'user': dict(project=['id'])})})
            mq.with_session(ssn).end().all()  # ERROR

        # ### User -> Article -> Comment: selectinload() -> selectinload()
        # This one WORKS!
        with QueryLogger(engine) as ql:
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           join={'comments': dict(project=['id'])})})
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0],
                             'FROM u',
                             'LIMIT 20')
            self.assertQuery(ql[1],
                             'FROM a' if SA_13 else 'JOIN a',
                             'WHERE group_row_n <= 10')
            self.assertQuery(ql[2],
                             'FROM c' if SA_13 else 'JOIN c',
                             'WHERE group_row_n <= 5')
            self.assertEqual(len(ql), 3)  # no error!


        with QueryLogger(engine) as ql:
            # Query limit: users
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           join={'comments': dict(project=['id'])}),},
                                    limit=3)
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0],
                             'LIMIT 3')  # <-- limit applied
            self.assertQuery(ql[1],
                             'WHERE group_row_n <= 10')
            self.assertQuery(ql[2],
                             'WHERE group_row_n <= 5')

        with QueryLogger(engine) as ql:
            # Query limit: articles
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           limit=3,
                                                           join={'comments': dict(project=['id'])})})
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0],
                             'LIMIT 20')
            self.assertQuery(ql[1],
                             'WHERE group_row_n <= 3')  # <-- limit applied
            self.assertQuery(ql[2],
                             'WHERE group_row_n <= 5')

        with QueryLogger(engine) as ql:
            # Query limit: comments
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           join={'comments': dict(project=['id'],
                                                                                  limit=3)})})
            mq.with_session(ssn).end().all()

            self.assertQuery(ql[0],
                             'LIMIT 20')
            self.assertQuery(ql[1],
                             'WHERE group_row_n <= 10')
            self.assertQuery(ql[2],
                             'WHERE group_row_n <= 3')  # <-- limit applied

        # ### Comment -> Article: joinedload()
        # Will fail: joinedload() does not support LIMIT
        with self.assertRaises(ValueError):
            mq = limited_c_mq.query(join={'article': dict(project=['id'])})
            mq.with_session(ssn).end().all()  # ERROR

        # ### Comment -> Article -> User: joinedload() -> joinedload()
        # Will fail: joinedload() does not support LIMIT
        with self.assertRaises(ValueError):
            mq = limited_c_mq.query(join={'article': dict(project=['id'],
                                                          join={'user': dict(project=['id'])})})
            mq.with_session(ssn).end().all()  # ERROR

        # ### Comment -> Article -> Comment: joinedload() -> selectinload()
        # Will fail: joinedload() does not support LIMIT
        with self.assertRaises(ValueError):
            mq = limited_c_mq.query(join={'article': dict(project=['id'],
                                                          join={'comments': dict(project=['id'])})})
            mq.with_session(ssn).end().all()  # ERROR


        # === Test: intermediate limit
        # Now let's try another case: there's a legitimate limit in the middle, and then another relationship
        # The first join uses selectinload() and therefore supports LIMITs
        # The second join, however, is a joinedload() to a LIMITed query
        limited_user_settings = MongoQuerySettingsDict(
            max_items=20,  # LEGITIMATE LIMIT
            allowed_relations=['articles'],
            related={
                # User -> Articles: one user has many articles
                # Join strategy: selectinload()
                'articles': lambda: MongoQuerySettingsDict(
                    max_items=10,  # LEGITIMATE LIMIT
                    allowed_relations=['user'],  # joinedload() relationship that won't support it
                ),
            }
        )

        limited_u_mq = Reusable(MongoQuery(models.User, limited_user_settings))
        
        # User -> Article -> User: selectinload() -> selectinload() -> joinedload()
        # This one WORKS!
        if SA_12:
            self.skipTest('Fails under SA 1.2.x; not going to fix!')
        with QueryLogger(engine) as ql:
            mq = limited_u_mq.query(join={'articles': dict(project=['id'],
                                                           join={'user': dict(project=['id'])})})
            mq.with_session(ssn).end().all()

            self.assertEqual(len(ql), 2)  # no error!

    @unittest.skipIf(SA_12, TEST_QUERY_STRING_ONLY_MATCHES_SA13)
    def test_selectinquery(self):
        """ Test join using the custom-made selectinquery() """
        u = models.User
        gw = models.GirlWatcher

        engine = self.engine
        ssn = self.Session()

        # Enable it, because setUp() has disabled it.
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

        # Helpers that will test the results
        all_users_with_articles = [
            dict(name='a', articles=[dict(title='10'), dict(title='11'), dict(title='12')]),
            dict(name='b', articles=[dict(title='20'), dict(title='21')]),
            dict(name='c', articles=[dict(title='30')]),
        ]
        pluck_users = lambda l: [mq.pluck_instance(i) for i in res]

        # === Test: filter, limit, join, filter, sort
        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn).query(
                project=['name'],
                filter={'age': {'$gte': 0}},
                join={'articles': dict(project=['title'],
                                       filter={'theme': {'$ne': 'biography'}},
                                       sort=['title+']
                                       )},
                sort=['age-', 'id+'],
                limit=10
            )
            res = mq.end().all()

            # Test results
            self.assertEqual(pluck_users(res), all_users_with_articles)

            # Query 1: Primary, User
            self.assertQuery(ql[0],
                             # condition on the outer query
                             'WHERE u.age >= 0',
                             # Ordering
                             'ORDER BY u.age DESC, u.id',
                             # Limit
                             'LIMIT 10'
                             )
            self.assertNotIn('JOIN', ql[0])  # there must be NO JOINS! selectinload() handles it
            self.assertSelectedColumns(ql[0],
                                       'u.id', 'u.name'  # PK, projected
                                       )

            # Query 2: selectin, Article
            self.assertQuery(ql[1],
                             # Querying directly
                             'FROM a',
                             # Custom condition
                             'WHERE a.uid IN (1, 2, 3) AND a.theme IS DISTINCT FROM biography',
                             # Custom ordering
                             # First: by ForeignKey (so that sqlalchemy has entities coming in nice order)
                             # Next: by our custom ordering
                             # v1.3.16: no longer ordering by the primary key
                             'ORDER BY a.uid, a.title' if SA_UNTIL('1.3.15') else 'ORDER BY a.title'
                             )

            self.assertSelectedColumns(ql[1],
                                       'a.id', 'a.uid', 'a.title',  # PK, FK, projected
                                       # Note that selectin_query() loader will always load the foreign key column.
                                       # There's no way around it, because it has to join entities for us.
                                       )

        # === Test: same relationship, different query
        # Make sure the query was not stored somewhere (e.g. Bakery), and a freshly made query is used
        with QueryLogger(engine) as ql:
            u.mongoquery(ssn).query(
                project=['name'],
                join={'articles': dict(project=['title'],
                                       filter={'theme': 'biography'},
                                       sort=['title-']
                                       )}
            ).end().all()

            # Query 2: selectin, Article
            self.assertQuery(ql[1],
                             'FROM a',
                             'WHERE a.uid IN (1, 2, 3) AND a.theme = biography',
                             # v1.3.16: no ordering by PK anymore
                             'ORDER BY a.uid, a.title DESC' if SA_UNTIL('1.3.15') else 'ORDER BY a.title DESC'
                             )
            self.assertSelectedColumns(ql[1],
                                       'a.id', 'a.title', 'a.uid',  # PK, FK, projected
                                       )

        # === Test: two relationships at the same time
        # This is tricky: we need two x-to-many relationships, because that's where our selectinquery() shines.
        # GirlWatcher is the only model that has that.

        with QueryLogger(engine) as ql:
            gw.mongoquery(ssn).query(
                project=['name'],
                join={'good': dict(project=['name'],
                                   filter={'age': {'$gt': 1}},
                                   sort=['age+']),
                      'best': dict(project=['name'],
                                   filter={'age': {'$gt': 2}},
                                   sort=['age-']),
                      }
            ).end().all()

            self.assertEqual(len(ql), 3)  # two relations, 3 queries

            # Note that the order is unpredictable, so we have to detect it
            first_query = 1
            second_query = 2
            if 'best = true' not in ql[second_query]:
                first_query = 2
                second_query = 1

            # Query 2: selectin, User through 'good'
            self.assertQuery(ql[first_query],
                             # Joins correctly
                             'FROM gw AS gw_1 '
                             'JOIN gwf AS gwf_1 ON gw_1.id = gwf_1.gw_id AND gwf_1.best = false '
                             'JOIN u ON gwf_1.user_id = u.id',
                             # selectinload
                             'WHERE gw_1.id IN (1, 2) '
                             # Filter correctly
                             'AND u.age > 1',
                             # Ordering is correct
                             # v1.3.16: no ordering by PK anymore
                             'ORDER BY gw_1.id, u.age' if SA_UNTIL('1.3.15') else 'ORDER BY u.age',
                             )
            self.assertSelectedColumns(ql[first_query],
                                       'gw_1.id',  # PK
                                       'u.id', 'u.name'  # PK, projected
                                       )

            # Query 3: selectin, User through 'best'
            self.assertQuery(ql[second_query],
                             # Joins correctly
                             'FROM gw AS gw_1 '
                             'JOIN gwf AS gwf_1 ON gw_1.id = gwf_1.gw_id AND gwf_1.best = true '
                             'JOIN u ON gwf_1.user_id = u.id',
                             # selectinload
                             'WHERE gw_1.id IN (1, 2) '
                             # Filter correctly
                             'AND u.age > 2',
                             # Ordering is correct
                             # v1.3.16: no ordering by PK anymore
                             'ORDER BY gw_1.id, u.age DESC' if SA_UNTIL('1.3.15') else 'ORDER BY u.age DESC',
                             )
            self.assertSelectedColumns(ql[second_query],
                                       'gw_1.id',  # PK
                                       'u.id', 'u.name'  # PK, projected
                                       )

        # === Test: 2 joins (selectin + left outer join), filters and projections
        # selectinquery() is used for articles
        # join() is used for user
        with QueryLogger(engine) as ql:
            u.mongoquery(ssn).query(
                project=['name'],
                filter={'age': 18},
                join={'articles': dict(project=['title'],
                                       filter={'theme': 'sci-fi'},
                                       join={'user': dict(project=['name'],
                                                          filter={'age': {'$gt': 18}})})}
            ).end().all()

            self.assertEqual(len(ql), 2)  # a relation, and a joined relation: 2 queries

            # Query 1: User, main
            self.assertQuery(ql[0],
                             'FROM u',
                             'u.age = 18'
                             )
            self.assertNotIn('JOIN', ql[0])

            # Query 2: Articles, selectinquery + join(user)
            self.assertQuery(ql[1],
                             'FROM a',
                             # Joined relation, with filter
                             'LEFT OUTER JOIN u AS u_1 ON u_1.id = a.uid AND u_1.age > 18',
                             # Filter
                             'WHERE a.uid IN (1, 2) AND a.theme = sci-fi',
                             )
            self.assertSelectedColumns(ql[1],
                                       'a.id', 'a.uid', 'a.title',  # PK, FK, project
                                       'u_1.id', 'u_1.name',  # PK, project
                                       )

        # === Test: 2 joins: selectinquery() + selectinload()
        # Old good selectinload() is used
        # There is no filter applied to Article.comments, so MongoJoin handler will choose selectinload()
        with QueryLogger(engine) as ql:
            u.mongoquery(ssn).query(
                project=['name'],
                filter={'age': 18},
                join={'articles': dict(project=['title'],
                                       filter={'theme': {'$ne': 'sci-fi'}},
                                       join=('comments',))}
            ).end().all()

            self.assertEqual(len(ql), 3, 'expected 3 queries in total')  # a relation, and a nested relation: 3 queries

        # === Test: 2 joins (selectinquery() + selectinquery()), filters and projections
        with QueryLogger(engine) as ql:
            u.mongoquery(ssn).query(
                project=['name'],
                filter={'age': 18},
                join={'articles': dict(project=['title'],
                                       filter={'theme': {'$ne': 'sci-fi'}},
                                       join={'comments': dict(project=['text'],
                                                              filter={'text': {'$exists': True}})})}
            ).end().all()

            self.assertEqual(len(ql), 3, 'expected 3 queries in total')  # a relation, and a nested relation: 3 queries

            # Query 1: User, main
            self.assertQuery(ql[0],
                             'FROM u',
                             'WHERE u.age = 18'
                             )
            self.assertNotIn('JOIN', ql[0])
            self.assertSelectedColumns(ql[0],
                                       'u.id', 'u.name'  # PK, project
                                       )

            # Query 2: Articles, selectin
            self.assertQuery(ql[1],
                             'FROM a',
                             # Filter
                             'WHERE a.uid IN (1, 2) AND a.theme IS DISTINCT FROM sci-fi',
                             # v1.3.16: no ordering by PK anymore
                             'ORDER BY a.uid' if SA_UNTIL('1.3.15') else '',
                             )
            self.assertSelectedColumns(ql[1],
                                       'a.id', 'a.uid', 'a.title',  # PK, FK, project
                                       )

            # Query 3: Comments, selectin
            self.assertQuery(ql[2],
                             'FROM c',
                             # Filter
                             'WHERE c.aid IN (10, 11, 12, 20, 21) AND c.text IS NOT NULL'
                             )
            self.assertSelectedColumns(ql[2],
                                       'c.id', 'c.aid', 'c.text'  # PK, FK, project
                                       )

        # === Test: reusing selectinload() many times over
        # SqlAlchemy reuses SelectInQueryLoader.
        # My old code installed a wrapper, and did it every time the query was executed.
        # As a result, the wrapper got re-wrapped every single time, and ultimately, Python gave the following exception:
        # RecursionError: maximum recursion depth exceeded while calling a Python object
        # This test is designed to check how selectinquery() behaves when re-used many times over.

        old_recursion_limit = sys.getrecursionlimit()
        # Let's choose just enough to detect a recursion
        # How did I choose the number?
        #   > [Previous line repeated 53 more times]
        #   > RecursionError: maximum recursion depth exceeded
        # "26" means we still have enough reserve for a few more calls when the code changes.
        # The smaller is the number, the fewer repetitions we need to hit it, the faster will the test be.
        sys.setrecursionlimit(200)

        for i in range(100):  # use half the recursionlimit
            u.mongoquery(ssn).query(
                project=['name'],
                filter={'age': 18},
                join={'articles': dict(project=['title'],
                                       filter={'theme': {'$ne': 'sci-fi'}},
                                       join={'comments': dict(project=['text'],
                                                              filter={'text': {'$exists': True}})})}
            ).end().all()

        sys.setrecursionlimit(old_recursion_limit)

    @unittest.skipIf(SA_12, TEST_QUERY_STRING_ONLY_MATCHES_SA13)
    def test_selectinquery_join_skip_limit(self):
        """ Test join + skip/limit

            This is an experimental method that only works with PostgreSQL and uses window functions
        """
        u = models.User

        engine = self.engine
        ssn = self.Session()

        # Enable it, because setUp() has disabled it.
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

        # === Test: joined one-to-many, LIMIT
        # Plan: User -> Articles: selectinload()
        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn).query(project=('id',),
                                         join={'articles': dict(project=('id', 'uid'),
                                                                sort=('id-',),
                                                                limit=1)},
                                         sort=('id-',),
                                         limit=2)
            res = mq.end().all()

            self.assertQuery(ql[0],
                             'FROM u',
                             'ORDER BY u.id DESC',
                             # Outer query: limit as usual
                             'LIMIT 2')

            self.assertQuery(ql[1],
                             # Rows in a window are numbered ; ordering is applied
                             'row_number() OVER (PARTITION BY a.uid ORDER BY a.id DESC) AS group_row_n',
                             # Limit works through an outside condition
                             'WHERE group_row_n <= 1',
                             )
            self.assertNotIn('LIMIT', ql[1])

            self.assertEqual(list(map(mq.pluck_instance, res)),
                             [{'id': 3, 'articles': [{'id': 30, 'uid': 3}]},
                              {'id': 2, 'articles': [{'id': 21, 'uid': 2}]}])

        # === Test: joined one-to-many, SKIP + LIMIT
        # Plan: User -> Articles: selectinload()
        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn).query(project=('id',),
                                         join={'articles': dict(project=('id', 'uid'),
                                                                sort=('id-',),
                                                                skip=1,
                                                                limit=1)},
                                         sort=('id-',),
                                         skip=1,
                                         limit=2)
            res = mq.end().all()

            self.assertQuery(ql[0],
                             'FROM u',
                             'ORDER BY u.id DESC',
                             # Outer query: limit & offset as usual
                             'LIMIT 2 OFFSET 1')

            self.assertQuery(ql[1],
                             # Limit & skip work through a condition
                             'WHERE group_row_n > 1 AND group_row_n <= 2',
                             )

            self.assertEqual(list(map(mq.pluck_instance, res)),
                             [{'id': 2, 'articles': [{'id': 21, 'uid': 2}]},
                              {'id': 1, 'articles': [{'id': 11, 'uid': 1}]}])

        # === Test: join + join, limits everywhere
        # This test will join 2 relationships to a limited query:
        # 1. 'comments', which will be loaded with selectinload()
        # 2. 'user', which will be loaded with a left join
        # Both are supposed to work fine, despite MongoLimit wrapping everything into a from_self() subquery
        # Plan: User -> Article -> Comment: selectinload() + selectinload()
        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn).query(project=('id',),
                                         join={'articles': dict(project=('id', 'uid'),
                                                                join={
                                                                    # one-to-many: using selectinquery()
                                                                    'comments': dict(project=('id', 'aid'),
                                                                                     sort=('id-',),
                                                                                     limit=1),
                                                                    # one-to-one: using left join
                                                                    'user': dict(project=('id',),)},
                                                                sort=('id-',),
                                                                limit=1)},
                                         sort=('id-',),
                                         limit=2)
            res = mq.end().all()

            self.assertQuery(ql[1],
                             # Rows in a window are numbered ; ordering is applied
                             'row_number() OVER (PARTITION BY a.uid ORDER BY a.id DESC) AS group_row_n',
                             # Limit works through an outside condition
                             'WHERE group_row_n <= 1',
                             # Joined users, properly aliased
                             'LEFT OUTER JOIN u AS u_1 ON u_1.id = anon_1.a_uid',
                             )

            self.assertQuery(ql[2],
                             # Rows numbered
                             'row_number() OVER (PARTITION BY c.aid ORDER BY c.id DESC) AS group_row_n',
                             # Limit with a condition
                             'WHERE group_row_n <= 1',
                             )
            from pprint import pprint
            pprint(list(map(mq.pluck_instance, res)))
            self.assertEqual(list(map(mq.pluck_instance, res)), [
                {'id': 3, 'articles': [{'id': 30, 'uid': 3, 'user': {'id': 3}, 'comments': []}]},
                {'id': 2, 'articles': [{'id': 21, 'uid': 2, 'user': {'id': 2}, 'comments': [{'aid': 21, 'id': 108}]}]},
            ])

    @unittest.skipIf(SA_12, TEST_QUERY_STRING_ONLY_MATCHES_SA13)
    def test_selectinquery_caching(self):
        """ Test how query caching works with selectinquery """
        # selectinquery() uses some smart query caching
        # However, it's not very straightforward:
        # the underlying BakedQuery caches the SQL query itself, and MongoQuery.from_query() lets you use any query.
        # In addition, different Query Objects can be used, which all results in complicated caching behavior.
        # Here, we're going to test it.
        
        # Prepare
        u = models.User
        engine = self.engine
        ssn = self.Session()

        # Enable it, because setUp() has disabled it.
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

        # === Test 1: Run the same QO on two different initial queries
        query_object = dict(
            project=['name'],
            filter={'age': {'$gte': 0}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': {'$ne': 'biography'}},
                                   )},
        )

        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn.query(u).filter_by(id=1)).query(**query_object)
            res = mq.end().all()

            # Query 1: Primary, User
            self.assertQuery(ql[0],
                             # The from_query() filter
                             'u.id = 1',
                             # condition on the outer query
                             'u.age >= 0',
                             )

        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn.query(u).filter_by(id=2)).query(**query_object)
            res = mq.end().all()

            # Query 1: Primary, User
            self.assertQuery(ql[0],
                             # The from_query() filter
                             'u.id = 2',  # CHANGED! not cached!
                             # condition on the outer query
                             'u.age >= 0',
                             )

        # === Test 2: querying with Postgres-specific features

        # BUG: It has turned out that when a query uses Postgres-specific features, it's unable to compile the query.
        # It looks like this:
        # > stmt_compiled = query.statement.compile()
        # > sqlalchemy.exc.UnsupportedCompilationError: Compiler can't render element of type <class 'sqlalchemy.dialects.postgresql.array.array'>
        # This was because the dialect was not provided.
        # Seems like MongoSQL is becoming increasingly Postgres-bound.

        # Query a model that has ARRAY fields
        with QueryLogger(engine) as ql:
            mq = u.mongoquery(ssn.query(u)).query(
                filter={'tags': {'$in': ['beautiful']}},
                # The bug only showed up when two relationships were joined
                # This happens because MongoJoin switches to SELECTINQUERY when two LEFT JOINS are added
                #join=['articles', 'comments'],
                # But we won't rely on that.
                # A more reproducible way to test that would be to force a SELECTINQUERY strategy:
                # for instance, by providing a Query Object! :)
                join={'articles': {'join':[]}, 'comments': {'join': []}},
            )
            res = mq.end().all()

            # Query 1:
            self.assertEqual(len(ql), 1)


    def test_join_when_fk_is_deferred(self):
        c = models.ManyForeignKeysModel

        # === Test: project + limit + join + project, one-to-one relationship
        mq = c.mongoquery().query(
            project=['name'],  # FK is deferred
            join={
                'user_1': dict(  # forces MongoJoin to use the LEFT OUTER JOIN join handler
                    project=['name']
                )
            },
            limit=10,  # forces MongoSQL to use a subquery
        )
        qs = self.assertQuery(mq.end(),
                              # Subquery
                              "FROM (SELECT mf.id",
                              # Limit, Aliased
                              "LIMIT 10) AS anon_1",
                              # Join condition references the alias
                              "anon_1 LEFT OUTER JOIN u AS u_1 ON u_1.id = anon_1.mf_user_1_id",
                              )
        self.assertSelectedColumns(qs,
                                   'u_1.id', 'u_1.name',  # PK + project
                                   'anon_1.mf_id', 'anon_1.mf_name',  # PK + project
                                   # And also this one undeferred field
                                   'anon_1.mf_user_1_id',
                                   )

        # The extra FK field is not specified in the projection
        self.assertEqual(mq.get_projection_tree(), {'name': 1, 'user_1': {'name': 1}})

    def test_sorting_when_column_is_deferred(self):
        c = models.ManyForeignKeysModel

        # === Test: project + limit + join + sort, one-to-one relationship
        mq = c.mongoquery().query(
            sort=['user_2_id-'],
            project=['name'],  # sorting key is deferred
            join={'user_1': dict(project=['name'])},  # forces MongoJoin to use the LEFT OUTER JOIN join handler
            limit=10,  # forces MongoJoin to use a subquery
        )
        qs = self.assertQuery(mq.end(),
                              # Subquery
                              "FROM (SELECT mf.id",
                              # Ordering, inside subquery
                              "FROM mf ORDER BY mf.user_2_id DESC",
                              # Limit, Aliased
                              "LIMIT 10) AS anon_1",
                              # Join condition references the alias
                              "anon_1 LEFT OUTER JOIN u AS u_1 ON u_1.id = anon_1.mf_user_1_id",
                              # Ordering on the outside too
                              "ORDER BY anon_1.mf_user_2_id DESC",
                              )

    def test_ensure_loaded(self):
        """ Test MongoQuery.ensure_loaded() """
        u = models.User

        # === Test: columns, relationship
        mq = u.mongoquery().query(
            project=['name'],
            filter={'age': {'$gt': 0}},
            join={'articles': dict(project=['title'])}
        )
        mq.ensure_loaded('age', 'comments',
                         # Related column!
                         'articles.data')

        self.assertEqual(mq.get_projection_tree(),
                         {'name': 1,
                          # 'age' is loaded, but quietly
                          'articles': {
                              'title': 1,
                              # 'data' is loaded, but quietly
                          },
                          # 'comments' not even mentioned
                          })

        # We can load it too!
        mq.ensure_loaded('articles')

        # === Test: can't use ensure_loaded() when filtering is present
        mq = u.mongoquery().query(
            project=['name'],
            join={'articles': dict(project=['title'], filter={'uid': 1})}
        )

        # Okay to call it without arguments
        mq.ensure_loaded()

        # Okay to load other relations
        mq.ensure_loaded('age', 'comments')

        # Can do it twice, no problem
        mq.ensure_loaded('age', 'comments')

        # Can't load it
        with self.assertRaises(InvalidQueryError):
            mq.ensure_loaded('articles')

        # === Test: handler_settings, columns, relationship
        # In this test, we pay special attention to `comment_calc` and make sure this projection does not disappear
        mq = MongoQuery(models.User, dict(
            related={
                'comments': dict(
                    default_exclude=('comment_calc',),
                )
            }
        ))
        mq.query(
            project=['name'],
            join={'comments': dict(project=['comment_calc'])}
        )
        mq.ensure_loaded('comments')

        self.assertEqual(mq.get_projection_tree(),
                         {'name': 1,
                          'comments': {
                              'comment_calc': 1,
                          },
                          })

    @unittest.skipIf(SA_12, 'AssociationProxy is only supported for SA 1.3.x and newer')
    def test_association_proxy(self):
        """ Test how MongoSQL deals with association proxy """
        g = models.GirlWatcher
        m = models.GirlWatcherManager

        engine = self.engine
        ssn = self.Session()

        # Enable it, because setUp() has disabled it.
        handlers.MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True

        # === Test: Filter
        mq = g.mongoquery().query(filter={'good_names': 'a'})
        self.assertQuery(mq.end(),
                         # Subquery filter
                         'WHERE EXISTS (SELECT 1',
                         # JOIN users
                         'FROM gwf, u',
                         # Join condition
                         'WHERE gw.id = gwf.gw_id AND gwf.best = false AND gwf.user_id = u.id',
                         # Filter: at least one
                         "AND u.name = 'a')",
                         literal=True)

        # === Test: Filter: $in
        mq = g.mongoquery().query(filter={'good_names': {'$in': ['a', 'b']}})
        self.assertQuery(mq.end(),
                         # Filter: IN
                         "AND u.name IN ('a', 'b'))",
                         literal=True)

        # === Test: Project
        with QueryLogger(engine) as ql:
            mq = g.mongoquery(ssn).query(project=['good_names'])
            res = mq.end().all()

            # First. Check projection
            projection = mq.get_full_projection_tree()
            self.assertEqual(projection['good_names'], 1)  # AssociationProxy included
            self.assertNotIn('good', projection)  # the relationship is not explicitly included

            # Query 1: simply id
            self.assertQuery(ql[0],
                             'SELECT gw.id',
                             'FROM gw')
            self.assertSelectedColumns(ql[0],
                                       'gw.id'  # PK only
                                       )

            # Query 2: loaded relationship
            self.assertQuery(ql[1],
                             # Loads the relationship
                             'FROM gw', 'JOIN gwf', 'JOIN u'
                             )
            self.assertSelectedColumns(ql[1],
                                       # Only includes the most important fields
                                       'gw_1.id', 'u.id', 'u.name',
                                       # And does not include: 'u.tags', 'u.age'
                                       )

        # === Test: project + join
        with QueryLogger(engine) as ql:
            mq = g.mongoquery(ssn).query(
                # project will require an association proxy, which in turn will load the relationship
                project=['good_names'],
                # But we also load the relationship explicitly, ourselves
                join=dict(
                    good=dict(
                        project=['name', 'age']
                    )
                )
            )
            res = mq.end().all()

            # Query 2: loaded relationship
            self.assertSelectedColumns(ql[1],
                                       'gw_1.id', 'u.id', 'u.name',
                                       # it also loads the extra field requested by `join`
                                       'u.age',
                                       )

        # === Test: project + join-filter
        with QueryLogger(engine) as ql:
            mq = g.mongoquery(ssn).query(
                project=['good_names'],
                join=dict(
                    good=dict(
                        project=['name', 'age'],
                        # Filter the relationship that is used by the association proxy
                        filter={'age': {'$gte': 18}}
                    )
                )
            )
            res = mq.end().all()

            # Query 2: loaded relationship
            self.assertQuery(ql[1],
                             # The condition is there
                             'WHERE gw_1.id IN (1, 2) AND u.age >= 18',
                             literal=True,
                             )

        # === Test: in join(): filter + project
        mq = m.mongoquery(ssn).query(
            project=['name'],
            join={
                'girlwatcher': dict(
                    project=['good_names'],
                    filter={'good_names': 'a'}
                )
            }
        )
        q = mq.end()

        # Query 1
        self.assertQuery(q,
                         # joining internally
                         'FROM gwm LEFT OUTER JOIN gw',
                         # filtering in the same query
                         '(EXISTS (SELECT 1'
                         )
        self.assertSelectedColumns(q,
                                   'gw_1.id', 'gwm.id', 'gwm.name')

        # === Test: in join(): filter + project & filter
        with QueryLogger(engine) as ql:
            mq = m.mongoquery(ssn).query(
                project=['name'],
                join={
                    'girlwatcher': dict(
                        project=['good_names'],
                        join=dict(
                            good=dict(
                                project=['name', 'age'],
                                filter={'age': {'$gte': 18}}
                            )
                        )
                    )
                }
            )
            res = mq.end().all()

            # Query 1
            self.assertQuery(ql[0],
                             # joining internally
                             'FROM gwm LEFT OUTER JOIN gw'
                             )
            self.assertNotIn(ql[0], 'u')  # users not loaded in this query; a second query is expected
            self.assertSelectedColumns(ql[0],
                                       'gw_1.id', 'gwm.id', 'gwm.name')

            # Query 2
            self.assertQuery(ql[1],
                             # Filtering condition is here
                             'WHERE gw_1.id IN (1, 2) AND u.age >= 18',
                             # v1.3.16: no ordering by PK anymore
                             'ORDER BY gw_1.id' if SA_UNTIL('1.3.15') else '',
                             )
            self.assertSelectedColumns(ql[1],
                                      'gw_1.id', 'u.id', 'u.name', 'u.age', 'u.master_id',
                                       # TODO: FIX: `u.tags` shoud NOT be included; but somehow, it does not
                                       #  currently work with aliased models. See mongosql.handlers.project.MongoProject._compile_relationship_options
                                       'u.tags' # not included
                                       )


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
                                       # side-effect: columns mentioned in ORDER BY are now included into the results
                                       'anon_1.a_theme',
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




# The default empty query object
# That's something get_final_query_object() would return when it has nothing else to do... :)
DEFAULT_QUERY_OBJECT = {
    **MongoQuery(models.User).query().get_final_query_object(),
    **dict(project={})
}

def override_default_query_object(**query_object):
    return {**DEFAULT_QUERY_OBJECT, **query_object}

