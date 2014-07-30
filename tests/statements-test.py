import unittest
import re
from collections import OrderedDict

from sqlalchemy.orm import Query
from sqlalchemy.dialects import postgresql as pg

from . import models


def q2sql(q):
    """ Convert an SqlAlchemy query to string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    stmt = q.statement
    dialect = pg.dialect()
    query = stmt.compile(dialect=dialect)
    return (query.string.encode(dialect.encoding) % query.params).decode(dialect.encoding)


class StatementsTest(unittest.TestCase):
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
        self.assertRaises(AssertionError, project, {'id': 1, 'name': 0})

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

        def test_filter(criteria, expected):
            qs = q2sql(filter(criteria))
            self.assertEqual(qs.partition('\nWHERE ')[2], expected)

        # Empty
        test_filter(None, 'true')
        test_filter({}, 'true')

        # Wrong
        self.assertRaises(AssertionError, test_filter, [1, 2], '')

        # Equality, multiple
        test_filter({'id': 1, 'name': 'a'}, '(u.id = 1 AND u.name = a)')
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
        test_filter({'$or': [{'id': 1, 'name': 'a'}, {'name': 'b'}]}, "((u.id = 1 AND u.name = a) OR u.name = b)")

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
        m = models.User

        # Okay
        mq = m.mongoquery(Query([models.User]))
        mq = mq.join(('articles', 'comments'))
        q = mq.end()
        qs = q2sql(q)
        self.assertIn('FROM u', qs)
        self.assertIn('LEFT OUTER JOIN a', qs)
        self.assertIn('LEFT OUTER JOIN c', qs)

        # Unknown relation
        mq = m.mongoquery(Query([models.User]))
        self.assertRaises(AssertionError, mq.join, ('???'))

    def test_aggregate(self):
        """ Test aggregate() """
        m = models.User

        aggregate = lambda agg_spec: m.mongoquery(Query([m])).project(('id',)).aggregate(agg_spec).end()

        def test_aggregate(agg_spec, expected_starts):
            qs = q2sql(aggregate(agg_spec))
            self.assertTrue(qs.startswith(expected_starts), '{!r} should start with {!r}'.format(qs, expected_starts))

        # Empty
        test_aggregate(None, 'SELECT u.id AS u_id \nFROM')
        test_aggregate({},   'SELECT u.id AS u_id \nFROM')

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
        test_aggregate(q, 'SELECT u.age AS age, count(*) AS n \nFROM')

        # $max(age), $sum(id=1 AND age >= 16)
        q = OrderedDict()  # OrderedDict() to have predictable output
        q['max_age'] = {'$max': 'age'}
        q['count'] = {'$sum': OrderedDict([('id', 1), ('age', {'$gte': 16})])}
        test_aggregate(q, 'SELECT max(u.age) AS max_age, sum(CAST((u.id = 1 AND u.age >= 16) AS INTEGER)) AS count \nFROM')

        # Unknown column
        self.assertRaises(AssertionError, test_aggregate, {'a': '???'}, '')
        self.assertRaises(AssertionError, test_aggregate, {'a': {'$max': '???'}}, '')
        self.assertRaises(AssertionError, test_aggregate, {'a': {'$sum': {'???': 1}}}, '')
