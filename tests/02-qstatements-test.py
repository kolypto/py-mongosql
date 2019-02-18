import unittest
import re
import sys
from collections import OrderedDict

from mongosql.statements import MongoFilter

from sqlalchemy.orm import Query
from sqlalchemy.dialects import postgresql as pg

from . import models
from .util import q2sql

MongoFilter.add_scalar_operator('$search', lambda col, val, oval: col.ilike('%{}%'.format(val)))


class QueryStatementsTest(unittest.TestCase):
    """ Test statements as strings """

    longMessage = True

    def test_projection(self):
        """ Test project() """
        m = models.User

        project = lambda projection: m.mongoquery(Query([m])).project(projection).end()

        def test_projection(projection, expected_columns):
            qs = q2sql(project(projection))
            rex = re.compile('u\.(\w+)[, ]')
            self.assertSetEqual(set(rex.findall(qs)), set(expected_columns), 'Expected {} in {}'.format(expected_columns, qs))

        # Empty values
        test_projection(None, ('id', 'name', 'tags', 'age'))
        test_projection([], ('id', 'name', 'tags', 'age'))
        test_projection({}, ('id', 'name', 'tags', 'age'))

        # Array syntax
        test_projection(['id'], ('id',))
        test_projection(['id', 'name'], ('id', 'name'))
        test_projection(['name'], ('id', 'name',))  # PK is always included :)
        self.assertRaises(AssertionError, project, ['id', 'lol'])

        # Object: inclusion
        test_projection({'id': 1}, ('id',))
        test_projection({'id': 1, 'name': 1}, ('id', 'name'))
        test_projection({'name': 1}, ('id', 'name',))

        # Object: exclusion
        test_projection({'id': 0}, ('id', 'name', 'tags', 'age'))
        test_projection({'id': 0, 'name': 0}, ('id', 'tags', 'age'))
        test_projection({'name': 0}, ('id', 'tags', 'age'))

        # Object: invalid
        self.assertRaises(AssertionError, project, {'id': 1, 'lol': 1})
        self.assertRaises(AssertionError, project, {'id': 0, 'lol': 0})

    def test_sort(self):
        """ Test sort() """
        m = models.User

        sort = lambda sort_spec: m.mongoquery(Query([m])).sort(sort_spec).end()

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
        self.assertRaises(AssertionError, test_sort, OrderedDict([['id', -2], ['age', -1]]), '')

    def test_group(self):
        """ Test group() """
        m = models.User

        group = lambda group_spec: m.mongoquery(Query([m])).group(group_spec).end()

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
        self.assertRaises(AssertionError, test_group, OrderedDict([['id', -2], ['age', -1]]), '')

    def test_filter(self):
        """ Test filter() """
        m = models.User

        filter = lambda criteria: m.mongoquery(Query([m])).filter(criteria).end()

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
        test_filter(None, 'true')
        test_filter({}, 'true')

        # Wrong
        self.assertRaises(AssertionError, test_filter, [1, 2], '')

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
        self.assertRaises(AssertionError, filter, {'tags': {'$in': 1}})
        test_filter({'name': {'$in': ['a', 'b', 'c']}}, 'u.name IN (a, b, c)')
        test_filter({'tags': {'$in': ['a', 'b', 'c']}}, 'u.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])')

        # $nin
        self.assertRaises(AssertionError, filter, {'tags': {'$nin': 1}})
        test_filter({'name': {'$nin': ['a', 'b', 'c']}}, 'u.name NOT IN (a, b, c)')
        test_filter({'tags': {'$nin': ['a', 'b', 'c']}}, 'NOT u.tags && CAST(ARRAY[a, b, c] AS VARCHAR[])')

        # $exists
        test_filter({'name': {'$exists': 0}}, 'u.name IS NULL')
        test_filter({'name': {'$exists': 1}}, 'u.name IS NOT NULL')

        # $all
        self.assertRaises(AssertionError, filter, {'name': {'$all': ['a', 'b', 'c']}})
        self.assertRaises(AssertionError, filter, {'tags': {'$all': 1}})
        test_filter({'tags': {'$all': ['a', 'b', 'c']}}, "u.tags @> CAST(ARRAY[a, b, c] AS VARCHAR[])")

        # $size
        self.assertRaises(AssertionError, filter, {'name': {'$size': 0}})
        test_filter({'tags': {'$size': 0}}, "array_length(u.tags, 1) IS NULL")
        test_filter({'tags': {'$size': 1}}, "array_length(u.tags, 1) = 1")

        # $or
        self.assertRaises(AssertionError, filter, {'$or': {}})
        test_filter({'$or': [{'id': 1}, {'name': 'a'}]}, "(u.id = 1 OR u.name = a)")

        # $and
        self.assertRaises(AssertionError, filter, {'$and': {}})
        test_filter({'$and': [{'id': 1}, {'name': 'a'}]}, "(u.id = 1 AND u.name = a)")

        # $nor
        self.assertRaises(AssertionError, filter, {'$nor': {}})
        test_filter({'$nor': [{'id': 1}, {'name': 'a'}]}, "NOT (u.id = 1 OR u.name = a)")

        # $not
        self.assertRaises(AssertionError, filter, {'$not': []})
        test_filter({'$not': {'id': 1}}, "u.id != 1")

        # Braces
        self.assertRaises(AssertionError, filter, {'$or': {}})
        # "((u.id = 1 AND u.name = a) OR u.name = b)")
        test_filter({'$or': [{'id': 1, 'name': 'a'}, {'name': 'b'}]}, ('u.id = 1', ' AND ', '.name = a', 'OR u.name = b'))

        # Custom filter
        test_filter({'name': {'$search': 'quer'}}, 'u.name ILIKE %quer%')

    def test_limit(self):
        """ Test limit() """
        m = models.User

        limit = lambda limit=None, skip=None: m.mongoquery(Query([m])).limit(limit, skip).end()

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
        q = m.mongoquery(q).limit(limit=15, skip=30).end()
        qs = q2sql(q)
        self.assertTrue(qs.endswith('LIMIT 15 OFFSET 30'), qs)

    def test_join(self):
        """ Test join() """

        # Two level join
        for sorting, desc in (('theme', ''), ('theme-', ' DESC'), ('theme+', '')):
            mq = models.Article.mongoquery(Query([models.Article]))
            mq = mq.query(project=['title'], outerjoin={'comments': {'project': ['aid'],
                                                                     'join': {'user': {'project': ['name']}}}}, limit=2, sort=[sorting])
            q = mq.end()
            qs = q2sql(q)
            self.assertIn('SELECT anon_1.a_id AS anon_1_a_id, anon_1.a_title AS anon_1_a_title, anon_1.a_theme AS anon_1_a_theme, u_1.id AS u_1_id, u_1.name AS u_1_name, c_1.id AS c_1_id, c_1.aid AS c_1_aid', qs)
            self.assertIn('FROM (SELECT a.id AS a_id', qs)
            self.assertIn('a ORDER BY a.theme{} \n LIMIT 2) AS anon_1 LEFT OUTER JOIN c AS c_1 ON anon_1.a_id = c_1.aid JOIN u AS u_1 ON u_1.id = c_1.uid'.format(desc), qs)
            self.assertTrue(qs.endswith('ORDER BY anon_1.a_theme{}'.format(desc)))
        # Three level join
        mq = models.Article.mongoquery(Query([models.Article]))
        mq = mq.query(project=['title'], outerjoin={'comments': {'project': ['aid'],
                                                    'join': {'user': {'project': ['name'],
                                                                      'join': {'roles': {'project': ['title']}}}
                                                    }}}, limit=2)
        q = mq.end()
        qs = q2sql(q)
        self._check_qs("""SELECT anon_1.a_id AS anon_1_a_id, anon_1.a_title AS anon_1_a_title, u_1.id AS u_1_id, u_1.name AS u_1_name, r_1.id AS r_1_id, r_1.title AS r_1_title, c_1.id AS c_1_id, c_1.aid AS c_1_aid
                           FROM (SELECT a.id AS a_id, a.title AS a_title
                              FROM a
                           LIMIT 2) AS anon_1 LEFT OUTER JOIN c AS c_1 ON anon_1.a_id = c_1.aid JOIN u AS u_1 ON u_1.id = c_1.uid JOIN r AS r_1 ON u_1.id = r_1.uid""",
                       qs)

        m = models.User

        # Okay
        mq = m.mongoquery(Query([models.User]))
        mq = mq.query(join={'articles': {'project': ('title',)}, 'comments': {}})
        q = mq.end()
        qs = q2sql(q)
        self.assertIn('FROM u', qs)
        self.assertIn('JOIN a', qs)
        self.assertIn('JOIN c', qs)
        self.assertNotIn('LEFT OUTER JOIN a', qs)
        self.assertNotIn('LEFT OUTER JOIN c', qs)

        # Left outer join
        mq = m.mongoquery(Query([models.User]))
        mq = mq.query(outerjoin={'articles': {'project': ('title',)}, 'comments': {}})
        q = mq.end()
        qs = q2sql(q)
        self.assertIn('FROM u', qs)
        self.assertIn('LEFT OUTER JOIN a', qs)
        self.assertIn('LEFT OUTER JOIN c', qs)

        # Unknown relation
        mq = m.mongoquery(Query([models.User]))
        self.assertRaises(AssertionError, mq.join, ('???'))

        # Join with limit, should use FROM (SELECT...)
        mq = models.Article.mongoquery(Query([models.Article]))
        mq = mq.query(project=['id'], outerjoin={'comments': {'project': ['id']}}, limit=2)
        q = mq.end()
        qs = q2sql(q)
        self._check_qs("""SELECT anon_1.a_id AS anon_1_a_id, c_1.id AS c_1_id
                          FROM (SELECT a.id AS a_id
                          FROM a
                          LIMIT 2) AS anon_1 LEFT OUTER JOIN c AS c_1 ON anon_1.a_id = c_1.aid""",
                       qs)

    def test_aggregate(self):
        """ Test aggregate() """
        m = models.User

        aggregate = lambda agg_spec: m.mongoquery(Query([m])).project(('id',)).aggregate(agg_spec).end()

        def test_aggregate(agg_spec, expected_starts):
            qs = q2sql(aggregate(agg_spec))
            self.assertTrue(qs.startswith(expected_starts), '{!r} should start with {!r}'.format(qs, expected_starts))

        def test_aggregate_qs(agg_spec, expected):
            qs = q2sql(aggregate(agg_spec))
            self._check_qs(expected, qs)

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
        q['age'] = 'age'  # column
        q['n'] = {'$sum': 1}
        test_aggregate_qs(q,
                          'SELECT \n'
                          'u.age AS age, \n'
                          'count(*) AS n \n'
                          'FROM'
                          )

        # $max(age), $sum(id=1 AND age >= 16)
        q = OrderedDict()  # OrderedDict() to have predictable output
        q['max_age'] = {'$max': 'age'}
        q['count'] = {'$sum': OrderedDict([('id', 1), ('age', {'$gte': 16})])}
        test_aggregate_qs(q,
                          'SELECT \n'
                          'max(u.age) AS max_age, \n'
                          'sum(CAST((u.id = 1 AND u.age >= 16) AS INTEGER)) AS count \n'
                          'FROM')

        # Unknown column
        self.assertRaises(AssertionError, test_aggregate, {'a': '???'}, '')
        self.assertRaises(AssertionError, test_aggregate, {'a': {'$max': '???'}}, '')
        self.assertRaises(AssertionError, test_aggregate, {'a': {'$sum': {'???': 1}}}, '')

    def test_filter_on_join(self):
        m = models.User
        mq = m.mongoquery(Query([models.User]))
        mq = mq.query(
            aggregate={'n': {'$sum': 1}},
            group=('name',),
            join={'articles': {'filter': {'title': {'$exists': True}}}}
        )
        q = mq.end()
        qs = q2sql(q)
        self._check_qs("""SELECT count(*) AS n
                          FROM u JOIN a AS a_1 ON u.id = a_1.uid
                          WHERE a_1.title IS NOT NULL
                          GROUP BY u.name""", qs)

        # Dotted syntax
        mq = m.mongoquery(Query([models.User]))
        mq = mq.query(filter={'articles.id': 1})
        q = mq.end()
        qs = q2sql(q)
        self.assertIn("WHERE EXISTS (SELECT 1 \nFROM a \nWHERE u.id = a.uid AND a.id = 1)", qs)
        mq = models.Comment.mongoquery(Query([models.Comment]))
        mq = mq.query(filter={'user.id': {'$gt': 2}})
        q = mq.end()
        qs = q2sql(q)
        self.assertIn("WHERE EXISTS (SELECT 1 \nFROM u \nWHERE u.id = c.uid AND u.id > 2)", qs)

        # Dotted multiple filter for same relation
        mq = models.Comment.mongoquery(Query([models.Comment]))
        mq = mq.query(filter={'user.id': {'$gt': 2}, 'user.age': {'$gt': 18}})
        q = mq.end()
        qs = q2sql(q)
        self.assertIn("WHERE EXISTS (SELECT 1 \nFROM u ", qs)
        self.assertIn("u.id = c.uid", qs)
        self.assertIn("u.id > 2", qs)
        self.assertIn("u.age > 18", qs)

    def test_join_multiple(self):
        """ Test join() same table multiple times"""

        mq = models.Edit.mongoquery(Query([models.Edit]))
        mq = mq.query(project=['id'], outerjoin={'user': {'project': ['name']},
                                                 'creator': {'project': ['id', 'tags'],
                                                             'filter': {'id': {'$lt': 1}}}})
        q = mq.end()

        qs = q2sql(q)
        self._check_qs("""
            SELECT  
                u_1.id AS u_1_id, 
                u_1.name AS u_1_name, 
                u_2.id AS u_2_id, 
                u_2.tags AS u_2_tags, 
                e.id AS e_id
        FROM e 
        LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid 
        LEFT OUTER JOIN u AS u_2 ON u_2.id = e.cuid AND u_2.id < 1""", qs)

    def _check_qs(self, should, qs):
        """ Compare a query line by line

            Problem: because of dict disorder, you can't just compare a query string: columns and expressions may be present,
            but be in a completely different order.
            Solution: compare a query piece by piece.
            To achieve this, you've got to feed the query as a string where every logical piece
            is separated by \n, and we compare the pieces
        """
        try:
            for line in should.splitlines():
                self.assertIn(line.strip().rstrip(','), qs)
        except:
            print(qs)
            raise

    @unittest.skip('join_hook() is now removed ; subclass MongoJoin in order to implement it')
    def test_join_hook_condition(self):
        """ Add condition on join. Used in filter joined entities
        For example for security reason."""

        mq = models.Edit.mongoquery(Query([models.Edit]))

        def join_hook_ripe_age(mjp):
            # type: (mongosql.statements.MongoJoinParams) -> lambda
            if mjp.target_model is models.User and mjp.relationship_name == 'user':
                return lambda x: x.filter(mjp.target_model_aliased.age > 18)
            return

        mq.on_join(join_hook_ripe_age)
        mq = mq.query(project=['id'], outerjoin={'user': {'project': ['name']},
                                                 'creator': {'project': ['id', 'tags']}})
        q = mq.end()

        qs = q2sql(q)
        # SELECT u_1.id AS u_1_id, u_1.name AS u_1_name,
        #        u_2.id AS u_2_id, u_2.tags AS u_2_tags,
        #        e.id AS e_id
        # FROM e
        #       LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid
        #       LEFT OUTER JOIN u AS u_2 ON u_2.id = e.cuid
        # WHERE u_1.age > 18
        # NOTE: have to check piece by piece because ordering is not guaranteed
        self.assertIn('SELECT u_1.', qs)
        self.assertIn('u_1.id AS u_1_id', qs)
        self.assertIn('u_1.name AS u_1_name', qs)
        self.assertIn('u_2.id AS u_2_id', qs)
        self.assertIn('u_2.tags AS u_2_tags', qs)
        self.assertIn('e.id AS e_id', qs)
        self.assertIn('FROM e LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid LEFT OUTER JOIN u AS u_2 ON u_2.id = e.cuid', qs)
        self.assertIn('WHERE u_1.age > 18', qs)

        # Without projections
        mq = models.Edit.mongoquery(Query([models.Edit]))
        mq.on_join(join_hook_ripe_age)
        mq = mq.query(project=['id'], outerjoin=['user'])
        q = mq.end()
        qs = q2sql(q)
        self.assertIn('u_1.id AS u_1_id', qs)
        self.assertIn('u_1.name AS u_1_name', qs)
        self.assertIn('u_1.tags AS u_1_tags', qs)
        self.assertIn('u_1.age AS u_1_age', qs)
        self.assertIn('e.id AS e_id', qs)
        self.assertIn('FROM e LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid', qs)
        self.assertIn('WHERE u_1.age > 18', qs)
        mq = models.Edit.mongoquery(Query([models.Edit]))

        def join_hook(name, rel, alias):
            if rel.property.mapper.class_  == models.Role:
                return lambda x: x.filter(alias.title.isnot(None))
            return
        mq.on_join(join_hook)
        mq = mq.query(project=['id'], outerjoin={'user': {'join': ['roles']}})

        q = mq.end()
        qs = q2sql(q)
        self._check_qs("""SELECT u_1.id AS u_1_id, u_1.name AS u_1_name, u_1.tags AS u_1_tags, u_1.age AS u_1_age, r_1.id AS r_1_id, r_1.uid AS r_1_uid, r_1.title AS r_1_title, r_1.description AS r_1_description, e.id AS e_id
                   FROM e LEFT OUTER JOIN u AS u_1 ON u_1.id = e.uid JOIN r AS r_1 ON u_1.id = r_1.uid
                   WHERE r_1.title IS NOT NULL""", qs)

    def test_get_project(self):
        m = models.User

        def _get_project(query):
            #return q2sql(m.mongoquery(Query([m])).query(**query).end())
            return m.mongoquery(Query([m])).query(**query).get_project()

        def _check_query(query, project):
            self.assertEqual(_get_project(query), project)

        _check_query({'project': ['id', 'name']}, {'id': 1, 'name': 1})
        _check_query({'project': {'id': 0, 'name': 0}}, {'id': 0, 'tags': 1, 'age': 1, 'name': 0, 'user_calculated': 1})
        _check_query({'project': {}}, {'id': 1, 'tags': 1, 'age': 1, 'name': 1})

        _check_query({'project': ['id', 'name'], 'join': ['roles']},
                     {'id': 1, 'name': 1, 'roles': {'id': 1, 'uid': 1, 'title': 1, 'description': 1}})
        _check_query({'project': ['id', 'name'], 'join': {'roles':{'project': ['title', 'description']}}},
                     {'id': 1, 'name': 1, 'roles': {'title': 1, 'description': 1}})
        _check_query({'project': ['id', 'name'], 'join': {'articles': {'project': ['title'], 'join': {'comments': {'project': ['uid', 'text']}}}}},
                     {'id': 1, 'name': 1, 'articles': {'title': 1, 'comments': {'uid': 1, 'text': 1}}})
        _check_query({'project': ['id', 'name'], 'join': {'articles': {'project': ['title'], 'join': ['comments']}}},
                     {'id': 1, 'name': 1, 'articles': {'title': 1, 'comments': {'aid': 1, 'id': 1, 'uid': 1, 'text': 1}}})
        _check_query({'project': ['id', 'name'], 'join': {'articles': {'project': ['title'], 'join': {'comments': {'project': {'uid': 0, 'text': 0}}}}}},
                     {'id': 1, 'name': 1, 'articles': {'title': 1, 'comments': {'aid': 1,  'id': 1,  'uid': 0,  'text': 0,  'comment_calc': 1}}})
        _check_query({'join': ['roles']},
                     {'id': 1, 'tags': 1, 'age': 1, 'name': 1, 'roles': {'id': 1, 'uid': 1, 'title': 1, 'description': 1}})

    def test_filter_hybrid(self):
        mq = models.Article.mongoquery(Query([models.Article]))
        query = mq.query(filter={'hybrid': True}).end()
        qs = q2sql(query)
        self._check_qs("""SELECT a.id, a.uid, a.title, a.theme, a.data
        FROM a
        WHERE (a.id > 10 AND (EXISTS (SELECT 1
        FROM u
        WHERE u.id = a.uid AND u.age > 18))) = true""", qs)
        self.assertRaises(AssertionError, mq.query, filter={'no_such_property': 1})
        self.assertRaises(AssertionError, mq.query, filter={'calculated': 10})

    def test_limit_with_filtered_join(self):
        m = models.User
        mq = m.mongoquery(Query([models.User]))
        mq = mq.query(limit=10, join={'articles': {'filter': {'title': {'$exists': True}}}})
        q = mq.end()
        qs = q2sql(q)
        self._check_qs("""SELECT anon_1.u_id AS anon_1_u_id, anon_1.u_name AS anon_1_u_name, anon_1.u_tags AS anon_1_u_tags, anon_1.u_age AS anon_1_u_age, a_1.id AS a_1_id, a_1.uid AS a_1_uid, a_1.title AS a_1_title, a_1.theme AS a_1_theme, a_1.data AS a_1_data
FROM (SELECT u.id AS u_id, u.name AS u_name, u.tags AS u_tags, u.age AS u_age
FROM u
WHERE EXISTS (SELECT 1
FROM a
WHERE u.id = a.uid AND a.title IS NOT NULL)
 LIMIT 10) AS anon_1 JOIN a AS a_1 ON anon_1.u_id = a_1.uid
WHERE a_1.title IS NOT NULL""", qs)
